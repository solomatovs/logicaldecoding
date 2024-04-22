use crate::model::{ChConnectorOpt, Column};
use postgres_protocol::message::backend::{
  LogicalReplicationMessage as PgReplication, XLogDataBody,
};
use std::thread;
use std::collections::BTreeMap;
use clickhouse_rs::Pool;
use clap::Parser;
use adaptive_backoff::prelude::*;
use tracing::{error, info, warn};
use tokio::sync::broadcast::{Receiver, error::TryRecvError};


pub struct ChBackend {
  rx: Receiver<XLogDataBody<PgReplication>>,
}

impl ChBackend {
  pub fn new(rx: Receiver<XLogDataBody<PgReplication>>) -> Self {
    Self {
      rx,
    }
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

      Self::wait_adapatitive_backoff(&mut backoff);
    }
  }

  async fn worker(&mut self) -> anyhow::Result<()> {

    let config = ChConnectorOpt::try_parse()?;
    let url = config.ch_url.to_string();
    let pool = Pool::new(url);
    let mut client = pool.get_handle().await?;
    let mut relations: BTreeMap<u32, Vec<Column>> = BTreeMap::default();

    loop {
      match self.rx.try_recv() {
        Err(TryRecvError::Closed) => {
          break;
        }
        Err(TryRecvError::Lagged(num)) => {
          error!("clickhouse lagged {num}");
          break;
        }
        Err(TryRecvError::Empty) => {},
        Ok(xlog_data) => {
          // let event = convert_replication_event(&relations, &xlog_data)?;
          info!("{:?}", xlog_data);
          match xlog_data.data() {
            PgReplication::Begin(val) => {
              
            }
            PgReplication::Insert(val) => {
             
            }
            PgReplication::Update(val) => {
          
            }
            PgReplication::Delete(val) => {
              
            }
            PgReplication::Relation(val) => {
              //relations.insert(val.rel_id, val.columns);
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

      // if self.sd.is_cancelled() {
      //   break;
      // }
    }
    

    Ok(())
  }
}