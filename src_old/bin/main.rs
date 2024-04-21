
use postgres_protocol::message::backend::{
  LogicalReplicationMessage as PgReplication, XLogDataBody,
};
use logicaldecoding::{PgBackend, ChBackend};
use anyhow;
use tracing::info;
use tracing_subscriber::EnvFilter;



pub fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .init();
  
  color_backtrace::install();

  let mut pg = PgBackend::new();
  
  let rt = tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .thread_name("worker")
    .build()?;

  let ctrl_c = tokio::signal::ctrl_c();
  let mut sigterm = {
      let _guard = rt.enter();
      tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap()
  };

  rt.block_on(async {
      tokio::select! {
          biased;
          _ = ctrl_c => {
              info!("ctrl-c received, shutting down");
          },
          _ = sigterm.recv() => {
              info!("sigterm received, shutting down");
          }
          _ = pg.start() => {

          }
      }
  });

  info!("done");
  Ok(())
}
