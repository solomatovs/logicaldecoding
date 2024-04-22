
use anyhow::anyhow;
use clap::Parser;
use std::io::ErrorKind;
use std::collections::BTreeMap;

use crate::ChConnectorOpt;
use crate::model::PgConnectorOpt;
use once_cell::sync::Lazy;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage as PgReplication, ReplicationMessage, XLogDataBody, RelationBody,
};
use tokio_postgres::tls::NoTlsStream;
use std::pin::Pin;
use std::str::FromStr;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::io::prelude::*;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::types::PgLsn;

use tokio_postgres::{Client, NoTls, Connection, Socket, SimpleQueryMessage};
use adaptive_backoff::prelude::*;
use tracing::{error, info, debug, trace, warn};
use futures_util::{pin_mut, TryStreamExt};

use clickhouse_rs::{Pool, ClientHandle, Client as ChClient, Block};


const TIME_SEC_CONVERSION: u64 = 946_684_800;
static EPOCH: Lazy<SystemTime> = Lazy::new(|| UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION));

/// connector to Postgres CDC.
pub struct PgBackend {
  // tx: Sender<XLogDataBody<PgReplication>>,
}

impl PgBackend {
  pub fn new() -> Self {
    Self {
      // tx,
    }
  }

  async fn create_postgres_client(config: &PgConnectorOpt) -> anyhow::Result<(Client, Connection<Socket, NoTlsStream>)> {
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

  fn slot_file_delete_if_exists(pg_slot: &String) -> anyhow::Result<()> {
    let res = std::fs::remove_file(pg_slot);
    match res {
      Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
      Err(err) => anyhow::bail!(err),
      Ok(res) => Ok(res),
    }
  }

  // fn lsn_file_truncate(pg_slot: &String) -> anyhow::Result<()> {
  //   std::fs::OpenOptions::new()
  //     .write(true)
  //     .create(true)
  //     .truncate(true)
  //     .open(pg_slot)?
  //     ;

  //   Ok(())
  // }

  fn lsn_file_write(pg_slot: &String, lsn: &PgLsn) -> anyhow::Result<()> {
    let lsn = lsn.to_string();
    let res = std::fs::OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(pg_slot)?
      .write_all(lsn.as_bytes())?
      ;

    Ok(res)
  }

  async fn create_replication_slot(repl_client: &Client, config: &PgConnectorOpt, pg_slot: &String) -> anyhow::Result<(PgLsn, String)> {
    let query = format!(r#"CREATE_REPLICATION_SLOT "{}"{} LOGICAL "pgoutput""#,
      pg_slot, config.temporary_slot_if_needed(),
    );
    let mut res = repl_client.simple_query(query.as_str()).await?.into_iter();
    loop {
      match res.next() {
        Some(SimpleQueryMessage::Row(row)) => {
          let consistent_point = row
            .try_get(1)?
            .map_or(Err(anyhow::anyhow!("{query} request did not return a value consistent_point")), |x| {
              let lsn = PgLsn::from_str(x).map_err(|x| anyhow!("{:?}", x));
              Ok(lsn?)
            })?
            ;
          
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
      
      match (lsn_server, lsn_file) {
        (None, None) => {
          let (cp, _) = Self::create_replication_slot(repl_client, config, &pg_slot).await?;
          Self::lsn_file_write(&pg_slot, &cp)?;
        }
        (Some(server), Some(file)) if server == file => {
          return Ok((server, None))
        }
        (Some(_), None) => {
          Self::drop_replication_slot(pg_slot, repl_client).await?;
        }
        (None, Some(_)) => {
          Self::slot_file_delete_if_exists(pg_slot)?
        }
        (_, _) => {
          Self::drop_replication_slot(pg_slot, repl_client).await?;
          Self::slot_file_delete_if_exists(pg_slot)?
        }
      }
    }
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

  async fn start_replication(slot_name: &String, last_lsn: &mut PgLsn, pg_publication: &String, client: &Client) -> anyhow::Result<LogicalReplicationStream> {
    let options = format!(
      r#"("proto_version" '1', "publication_names" '{}')"#,
      pg_publication
    );

    let query = format!(
        r#"START_REPLICATION SLOT "{}" LOGICAL {} {}"#,
        slot_name, last_lsn, options
    );

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
          error!("{err}");
      }

      Self::wait_adapatitive_backoff(&mut backoff);
    }
  }

  async fn worker(&mut self) -> anyhow::Result<()> {
    let config = PgConnectorOpt::from_args_safe()?;

    let (pg_client, conn) = config
      .pg_url
      .as_str()
      .parse::<tokio_postgres::Config>()?
      .replication_mode(ReplicationMode::Logical)
      .connect(NoTls).await?;
    
    tokio::spawn(conn);

    let pg_slot = config.get_slot_name_from_config_or_generate_if_not_provided();
    let pg_publication = config.get_publication_name_from_config_or_generate_if_not_provided();
    
    let (mut consistent_point, _) = Self::get_consistent_checkpoint(
      &pg_slot,
      &pg_client, 
      &config, 
    ).await?;
    
    let stream = Self::start_replication(
      &pg_slot,
      &mut consistent_point,
      &pg_publication,
      &pg_client,
    ).await?;
    tokio::pin!(stream);

    let url = ChConnectorOpt::try_parse()?.ch_url.to_string();
    let pool = Pool::new(url);
    let mut ch_client = pool.get_handle().await?;

    let mut payload : BTreeMap<u32, (RelationBody, Block)> = BTreeMap::new();

    while let Some(replication_message) = stream.try_next().await? {
      self.process_event(
        stream.as_mut(),
        replication_message,
        &mut consistent_point,
        &mut ch_client,
        &mut payload,
      )
      .await?;
    }

    Ok(())
  }

  async fn process_event(&mut self,
    stream: Pin<&mut LogicalReplicationStream>,
    xlog: ReplicationMessage<PgReplication>,
    last_lsn: &mut PgLsn,
    client: &mut ClientHandle,
    payload: &mut BTreeMap<u32, (RelationBody, Block)>,
  ) -> anyhow::Result<()> {
    match xlog {
      ReplicationMessage::XLogData(xlog_data) => {
        self.clickhouse_process(
          client,
          xlog_data,
          payload
        ).await;
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

  pub async fn clickhouse_process(&mut self,
    client: &mut ClientHandle,
    xlog_data: XLogDataBody<PgReplication>,
    payload: &mut BTreeMap<u32, (RelationBody, Block)>,
  ) {
    debug!("{:?}", xlog_data);
    match xlog_data.data() {
      PgReplication::Begin(val) => {
        
      }
      PgReplication::Insert(val) => {
        if let Some((rel, (r, b))) = payload.get_key_value(&val.rel_id()) {

        }
        let tu = val.tuple();
        
        //ch_blocks.insert(val.rel_id(), value);
      }
      PgReplication::Update(val) => {
        
      }
      PgReplication::Delete(val) => {
        
      }
      PgReplication::Relation(val) => {
        let block = Block::new();
        for c in val.columns() {
          println!("{:?}", c.name());
          // block.add_column(c.name(), values);
        }
        payload.insert(val.rel_id(), (val.clone(), Block::new()));
      }
      PgReplication::Commit(val) => {
        // send to postgres confirm last_lsn
        // *last_lsn = val.commit_lsn.into();
        
      }
      val => {
        warn!("unknown message: {:?}", val);
      }
    }
  }
}

