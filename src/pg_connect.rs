use crate::convert::convert_replication_event;
use anyhow::anyhow;
use crate::error::Error;
use clap::builder::FalseyValueParser;
use clap::{Parser, FromArgMatches, Args};
use std::io::ErrorKind;

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
use std::fs::File;
use std::io::prelude::*;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls, Connection, Socket, SimpleQueryMessage};
// use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use adaptive_backoff::prelude::*;
use tracing::{error, info, trace, debug, warn};
use futures_util::{pin_mut, TryStreamExt};


const TIME_SEC_CONVERSION: u64 = 946_684_800;
static EPOCH: Lazy<SystemTime> = Lazy::new(|| UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION));

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

  async fn get_confirmed_flush_lsn_from_pg_replication_slots(config: &PgConnectorOpt, pg_slot: &String) -> anyhow::Result<Option<PgLsn>> {
    let (client, conn) = Self::create_postgres_client(&config).await?;
    tokio::spawn(conn);

    let query: String = format!("select active, wal_status, restart_lsn, confirmed_flush_lsn from pg_replication_slots where slot_name = $1").into();
    let res = client.query_raw(query.as_str(), &[&pg_slot]).await?;
    pin_mut!(res);

    while let Some(x) = res.try_next().await? {
      let active: bool = x.try_get(0)?;
      let wal_status: String = x.try_get(1)?;
      let restart_lsn = x.try_get::<usize, PgLsn>(2)?;
      let confirmed_flush_lsn: PgLsn = x.try_get(3)?;
      
      return Ok(Some(confirmed_flush_lsn));
    }

    Ok(None)
  }

  async fn slot_file_get_confirmed_flush_lsn(pg_slot: &String) -> anyhow::Result<Option<PgLsn>> {
    match std::fs::read_to_string(pg_slot) {
      Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
      Err(e) => Err(anyhow!("{e}")),
      Ok(buf) if buf.is_empty() => Ok(None),
      Ok(buf) => {
        let buf = PgLsn::from_str(buf.as_str())
          .map_err(|x| anyhow!(
"error reading lsn number from file {}.
line was read: {}.
parsing error: {:?}", pg_slot, buf, x)
          )?;
        Ok(Some(buf))
      },
    }
  }

  fn slot_file_delete(pg_slot: &String) -> anyhow::Result<()> {
    Ok(std::fs::remove_file(pg_slot)?)
  }

  fn lsn_file_truncate(pg_slot: &String) -> anyhow::Result<()> {
    std::fs::OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(pg_slot)?
      ;

    Ok(())
  }

  async fn create_replication_slot(repl_client: &Client, config: &PgConnectorOpt, pg_slot: &String) -> anyhow::Result<(String, String)> {
    let query = format!(r#"CREATE_REPLICATION_SLOT "{}"{} LOGICAL "pgoutput""#,
      pg_slot, config.temporary_slot_if_needed(),
    );
    let mut res = repl_client.simple_query(query.as_str()).await?.into_iter();
    loop {
      match res.next() {
        Some(SimpleQueryMessage::Row(row)) => {
          let consistent_point = row
            .try_get(1)?
            .map_or(Err(anyhow::anyhow!("{query} request did not return a value consistent_point")), |x| Ok(x))?
            .to_string();
          
          let snapshot_name = row
          .try_get(2)?
          .map_or(Err(anyhow::anyhow!("{query} request did not return a value snapshot_name")), |x| Ok(x))?
          .to_string();
          
          return  Ok((consistent_point, snapshot_name))
        }
        Some(_) => continue,
        None => return Err(anyhow::anyhow!("{query} request did not return a value. res: {:?}", res))
      }
    }
  }

  pub async fn get_consistent_checkpoint(pg_slot: &String, repl_client: &Client, config: &PgConnectorOpt) -> anyhow::Result<(PgLsn, Option<String>)> {
    loop {
      let lsn_server = Self::get_confirmed_flush_lsn_from_pg_replication_slots(config, &pg_slot).await?;
      let lsn_file = Self::slot_file_get_confirmed_flush_lsn(&pg_slot).await?;
      
      // нормальная ситуация это когда файл слота и слот на сервере имеют одинаковые lsn номера
      // в любой нестандартной ситуации удаляем слот и запускаем resync
      match (lsn_server, lsn_file) {
        (None, None) => {
          let (consistent_point, snapshot_name) = Self::create_replication_slot(repl_client, config, &pg_slot).await?;
        }
        (None, Some(file)) => {
          Self::drop_replication_slot(pg_slot, repl_client).await?
        }
        (Some(server), None) => {
          
        }
        (Some(server), Some(file)) => {
          if server == file {
            info!("");
          }
          todo!()
        }
      }
    }

    
//     match consistent_point {
//       Some(val) => Ok((val, None)),
//       None => match &config.pg_consistent_point {
//         Some(val) => Ok((val.clone(), None)),
//         None => return Err(anyhow::anyhow!(r#"Lsn number not provided.
// LSN number search order:
//   1. if CREATE_REPLICATION_SLOT is executed, then lsn number is selected from the query result
//   2. otherwise selected from the config"#
//         )),
//       }
//     }

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

    todo!()
  }

  async fn drop_replication_slot(pg_slot: &String, repl_client: &Client) -> anyhow::Result<()> {
    let query = format!(r#"DROP_REPLICATION_SLOT "{}" WAIT"#, pg_slot);
    let res = repl_client.simple_query(query.as_str())
      .await?
      .into_iter()
      .filter_map(|msg| match msg {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
      })
      .collect::<Vec<_>>()
      .first()
    ;

    Ok(())
  }

  async fn start_replication(slot_name: &String, last_lsn: &mut PgLsn, client: &Client) -> anyhow::Result<LogicalReplicationStream> {
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
    info!("Running replication query - {query}");

    let stream = client.copy_both_simple::<bytes::Bytes>(&query).await?;
    let stream = LogicalReplicationStream::new(stream);

    Ok(stream)
  }

  fn backof_build() -> anyhow::Result<ExponentialBackoff> {
    ExponentialBackoffBuilder::default()
      .min(std::time::Duration::from_secs(1))
      .max(std::time::Duration::from_secs(10))
      .build()
      .map_err(|err| anyhow::anyhow!("{}", err))
  }

  fn wait_adapatitive_backoff(backoff: &mut ExponentialBackoff) {
    let wait_time = backoff.wait();
    info!(wait_time_seconds=%wait_time.as_secs(), "Waiting before retrying");
    thread::sleep(wait_time);
  }

  pub async fn start(&mut self) -> anyhow::Result<()> {
    let mut backoff = Self::backof_build()?;

    loop {
      if let Err(err) = self.worker().await {
          error!("{}", err);
      }

      if self.sd.is_cancelled() {
        break;
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

    let pg_slot = config.get_slot_name_from_config_or_generate_if_not_provided();

    let (mut consistent_point, snapshot_name) = Self::get_consistent_checkpoint(
      &pg_slot,
      &client, 
      &config, 
    ).await?;

    //let mut last_lsn = PgLsn::from_str(consistent_point.as_str()).map_err(|_| self::Error::ParseLsnError(consistent_point))?;
    let mut relations: BTreeMap<u32, Vec<Column>> = BTreeMap::default();

    let stream = Self::start_replication(
      &pg_slot,
      &mut consistent_point,
      &client,
    ).await?;
    tokio::pin!(stream);

    while let Some(replication_message) = stream.try_next().await? {
        let result = self.process_event(
          stream.as_mut(),
          &mut relations,
          replication_message,
          &mut consistent_point
        )
        .await;

        if let Err(e) = result {
            error!("PgConnector error: {e}");
            return Err(e);
        }

        if self.sd.is_cancelled() {
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
            info!("commit: {:?}", val);
            *last_lsn = val.commit_lsn.into();
          }
          val => {
            warn!("unknown message: {:?}", val);
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

