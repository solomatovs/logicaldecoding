use crate::model::ChConnectorOpt;

use std::thread;
use clickhouse_rs::Pool;
use clap::Parser;
use adaptive_backoff::prelude::*;
use tracing::{error, info};
use tokio_util::sync::CancellationToken;


pub struct ChConnector {
  sd: CancellationToken,
}

impl ChConnector {
  pub fn new(sd: CancellationToken) -> Self {
    Self {
      sd,
    }
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

    let config = ChConnectorOpt::try_parse()?;
    let url = config.ch_url.to_string();
    let pool = Pool::new(url);
    let mut client = pool.get_handle().await?;

    while !self.sd.is_cancelled() {
      client.ping().await?;
    }

    Ok(())
  }
}