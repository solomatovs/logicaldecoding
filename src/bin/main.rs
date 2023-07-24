
// use logicaldecoding::model::{PgConnectorOpt, ChConnectorOpt};
use logicaldecoding::{PgServer, ChConnector};
use anyhow;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use tokio;
use tokio::signal;
use tokio_util::sync::CancellationToken;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .init();
  
  color_backtrace::install();
  
  let token = CancellationToken::new();
  // let mut ch = ChConnector::new(token.child_token());
  let mut pg = PgServer::new(token.child_token());

  // let ch = tokio::spawn(async move {
  //   ch.start().await
  // });
  let pg = tokio::spawn(async move {
    pg.start().await
  });

  info!("press ctrl+c for shutdown");
  signal::ctrl_c().await?;
  info!("signal ctrl+c recived");
  info!("start canceling tasks");
  token.cancel();

  // if let Err(e) = ch.await? {
  //   error!("{:?}", e)
  // }
  if let Err(e) = pg.await? {
    error!("{:?}", e)
  }
  info!("finished canceling tasks");

  info!("done");
  Ok(())
}



// pub async fn start(kill: Receiver<()>) -> Result<(), std::io::Error> {
  

//   tokio::select! {
//     x = ch.start() => {
//       println!("clickhouse task shutdown");
//       x
//     },
//       _ = kill.recv() => Err(...),
//   }
// }


// async fn postgres_worker(token: CancellationToken) -> anyhow::Result<()> {
//     let mut backoff = ExponentialBackoffBuilder::default()
//         .min(std::time::Duration::from_secs(1))
//         .max(std::time::Duration::from_secs(60))
//         .build()
//         .map_err(|err| anyhow::anyhow!("{}", err))?;

//     loop {
//       let config = PgConnectorOpt::from_args();
//       let mut connector = match PgConnector::new(config.clone()).await {
//           Ok(connector) => {
//               backoff.reset();
//               connector
//           }
//           Err(e) => {
//               error!(%e,"error creating postgres connection");
//               wait_adapatitive_backoff(&mut backoff);
//               continue;
//           }
//       };

//       if let Err(err) = connector.process_stream().await {
//           error!(%err, "error handling postgres stream");
//       }

//       wait_adapatitive_backoff(&mut backoff);
//     }
// }

// async fn clickhouse_worker(token: CancellationToken) -> anyhow::Result<()> {
//   let mut backoff = ExponentialBackoffBuilder::default()
//       .min(std::time::Duration::from_secs(1))
//       .max(std::time::Duration::from_secs(60))
//       .build()
//       .map_err(|err| anyhow::anyhow!("{}", err))?;

//   while !token.is_cancelled() {
//     let config = ChConnectorOpt::from_args();
//     let mut connector = match ChConnector::new(config.clone()).await {
//         Ok(connector) => {
//             backoff.reset();
//             connector
//         }
//         Err(e) => {
//             error!(%e,"error creating postgres connection");
//             wait_adapatitive_backoff(&mut backoff);
//             continue;
//         }
//     };

//     if let Err(err) = connector.process_stream(&token).await {
//       error!(%err, "error handling clickhouse stream");
//     }
    
//     wait_adapatitive_backoff(&mut backoff);
//   }

//   Ok(())
// }

