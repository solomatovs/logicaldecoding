use crate::convert::convert_replication_event;
use crate::error::Error;
use clap::{Parser, FromArgMatches, Args};
use crate::model::{Column, LogicalReplicationMessage, PgConnectorOpt, CreateReplicationSlotResult};
use once_cell::sync::Lazy;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage as PgReplication, ReplicationMessage,
};
use tokio_postgres::tls::NoTlsStream;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::str::FromStr;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls, Connection, Socket};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use adaptive_backoff::prelude::*;
use tracing::{error, info, trace, debug, warn};


const TIME_SEC_CONVERSION: u64 = 946_684_800;
static EPOCH: Lazy<SystemTime> =
    Lazy::new(|| UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION));

/// connector to Postgres CDC.
pub struct PgServer {
  sd: CancellationToken,
}


impl PgServer {
  pub fn new(sd: CancellationToken) -> Self {
    Self {
      sd,
    }
  }

  // pub async fn create_pg_connector(config: PgConnectorOpt) -> anyhow::Result<PgConnector> {
  //   Self::create_replication_slot(&config).await?;

  //   let mut lsn: Option<PgLsn> = None;

  //   let (pg_client, conn) = config
  //       .url
  //       .as_str()
  //       .parse::<tokio_postgres::Config>()?
  //       .replication_mode(ReplicationMode::Logical)
  //       .connect(NoTls)
  //       .await?;
  //   tokio::spawn(conn);
  //   info!("Connected to Postgres");

  //   Ok(PgConnector {
  //       config,
  //       pg_client,
  //       lsn,
  //       relations: BTreeMap::default(),
  //   })
  // }

  async fn create_postgres_client(config: &PgConnectorOpt) -> anyhow::Result<(Client, Connection<Socket, NoTlsStream>)>
  {
    let config = config
      .pg_url
      .as_str()
      .parse::<tokio_postgres::Config>()?;


    let (pg_client, conn) = config
      .connect(NoTls)
      .await?;

    Ok((pg_client, conn))
  }

  pub async fn create_replication_slot_if_needed(repl_client: &Client, config: &PgConnectorOpt) -> anyhow::Result<(String, String)> {
    let (client, conn) = Self::create_postgres_client(&config).await?;
    tokio::spawn(conn);
    
    let pg_slot = match &config.pg_slot {
      Some(slot) => slot.clone(),
      None => {
        "slot_".to_owned()
        + &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string()
      }
    };

    let mut pg_consistent_point = match &config.pg_consistent_point {
      Some(pg_consistent_point) => pg_consistent_point.clone(),
      None => "0/0".into(),
    };

    let mut pg_snapshot_name = None;

    let query ="SELECT slot_name FROM pg_replication_slots where slot_name=$1";
    let res = client.query(query, &[&pg_slot]).await?;
    if res.is_empty() {
      info!("slot with name '{pg_slot}' doesn't exists. creating...");

      let temp = config.pg_slot_temp.unwrap_or(false);
      let temp = if temp {" TEMPORARY"} else {""};

      let query = format!(r#"CREATE_REPLICATION_SLOT "{}"{} LOGICAL "pgoutput" EXPORT_SNAPSHOT"#,
        pg_slot, temp,
      );

      let res = repl_client.query(query.as_str(), &[]).await?;
      let res = CreateReplicationSlotResult::try_from(res)?;
      info!("slot '{pg_slot}' created");

      pg_consistent_point = res.consistent_point;
      pg_snapshot_name = res.snapshot_name;
    }

    // if let Some(publication_name) = config.pg_publication {
    //   info!("Querying publications {publication_name}");
    //   let publications_query = "SELECT * from pg_publication WHERE pubname=$1";
    //   let publications = repl_client.query(publications_query, &[&publication_name]).await?;
    //   if publications.is_empty() {
    //       info!("Creating publication {publication_name}");

    //       let query = format!(r#"CREATE PUBLICATION "{}" FOR ALL TABLES"#, publication_name);
    //       let _query_out = repl_client.query(query.as_str(), &[]).await?;
    //   }
    // }

    Ok((pg_slot, pg_consistent_point))
  }

  pub async fn delete_replication_slot_if_exists(config: &PgConnectorOpt) -> anyhow::Result<()> {
    let (pg_client, conn) = Self::create_postgres_client(&config).await?;
    tokio::spawn(conn);

    if let Some(publication) = &config.pg_publication {
      let query = format!(r#"DROP PUBLICATION IF EXISTS "{}""#, publication);
      let _query_out = pg_client.query(query.as_str(), &[]).await?;
    }
    
    if let Some(slot) = &config.pg_slot {
      let query = format!("SELECT pg_drop_replication_slot('{}')", slot);
      let _query_out = pg_client.query(query.as_str(), &[]).await?;
    }

    Ok(())
  }

  async fn start_replication(repl_client: &Client, slot_name: &String, last_lsn: &mut PgLsn) -> anyhow::Result<LogicalReplicationStream> {
    // let options = format!(
    //   r#"("proto_version" '1', "publication_names" '{}')"#,
    //   config.pg_publication
    // );
    let options = format!(
      r#"("proto_version" '1')"#
    );
    let query = format!(
        r#"START_REPLICATION SLOT "{}" LOGICAL {} {}"#,
        slot_name, last_lsn, options
    );
    info!("Running replication query - {}", query);

    let stream = repl_client
        .copy_both_simple::<bytes::Bytes>(&query)
        .await?;
    
    let stream = LogicalReplicationStream::new(stream);

    Ok(stream)
  }

  fn wait_adapatitive_backoff(backoff: &mut ExponentialBackoff) {
    let wait_time = backoff.wait();
    info!(wait_time_seconds=%wait_time.as_secs(), "Waiting before retrying");
    thread::sleep(wait_time);
  }

  pub async fn start(&mut self) -> anyhow::Result<()> {
    let mut backoff = ExponentialBackoffBuilder::default()
      .min(std::time::Duration::from_secs(1))
      .max(std::time::Duration::from_secs(10))
      .build()
      .map_err(|err| anyhow::anyhow!("{}", err)).unwrap();

    while !self.sd.is_cancelled() {
      if let Err(err) = self.worker().await {
          error!("{}", err);
      }

      Self::wait_adapatitive_backoff(&mut backoff);
    }
    
    Ok(())
  }

  async fn worker(&mut self) -> anyhow::Result<()> {
    let config = PgConnectorOpt::from_args_safe()?;

    let (client, conn) = config
      .pg_url
      .as_str()
      .parse::<tokio_postgres::Config>()?
      .replication_mode(ReplicationMode::Logical)
      .connect(NoTls)
      .await?;
    tokio::spawn(conn);

    let (slot, consistent_point) = Self::create_replication_slot_if_needed(
      &client, 
      &config, 
    ).await?;

    let mut last_lsn = PgLsn::from_str(consistent_point.as_str()).map_err(|_| self::Error::ParseLsnError(consistent_point))?;
    let mut relations: BTreeMap<u32, Vec<Column>> = BTreeMap::default();

    let stream = Self::start_replication(
      &client, 
      &slot,
      &mut last_lsn
    ).await?;
    tokio::pin!(stream);

    while let Some(replication_message) = stream.try_next().await? {
        let result = self.process_event(
          stream.as_mut(),
          &mut relations,
          replication_message,
          &mut last_lsn
        )
        .await;

        if let Err(e) = result {
            error!("PgConnector error: {:#}", e);
        }

        if self.sd.is_cancelled() {
          info!("postgres worker shutdown...");
          break;
        }
    }

    Ok(())
  }

  async fn process_event(&mut self,
    stream: Pin<&mut LogicalReplicationStream>,
    relations: &mut BTreeMap<u32, Vec<Column>>,
    event: ReplicationMessage<PgReplication>,
    last_lsn: &mut PgLsn
  ) -> anyhow::Result<()> {
      match event {
          ReplicationMessage::XLogData(xlog_data) => {
              let event = convert_replication_event(&relations, &xlog_data)?;
              let json = serde_json::to_string(&event)?;
              
              match event.message {
                LogicalReplicationMessage::Begin(val) => {
                  info!("begin: {:?}", val);
                }
                LogicalReplicationMessage::Insert(val) => {
                  info!("insert: {:?}", val);
                }
                LogicalReplicationMessage::Update(val) => {
                  info!("update: {:?}", val);
                }
                LogicalReplicationMessage::Delete(val) => {
                  info!("delete: {:?}", val);
                }
                LogicalReplicationMessage::Relation(val) => {
                  info!("relation: {:?}", val);
                  relations.insert(val.rel_id, val.columns);
                }
                LogicalReplicationMessage::Commit(val) => {
                  info!("commit: {:?}", json);
                  *last_lsn = val.commit_lsn.into();
                }
                val => {
                  info!("unknown message: {:?}", val);
                }
              }
          }
          ReplicationMessage::PrimaryKeepAlive(keepalive) => {
              if keepalive.reply() == 1 {
                  debug!("Sending keepalive response");
                  let ts = EPOCH.elapsed().unwrap().as_micros() as i64;
                  stream
                      // .as_mut()
                      .standby_status_update(*last_lsn, *last_lsn, *last_lsn, ts, 0)
                      .await?;
              }
          }
          e => {
              info!("Unhandled event {:?}", e);
          }
      }

      Ok(())
    }
}

