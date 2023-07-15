mod replication;
mod types;
use std::env;
use std::error::Error;
use anyhow::__private::kind::TraitKind;

use anyhow::Result;
use replication::Transaction;
use sqlx::{migrate::Migrator, PgPool};
use tokio::task;
use tracing::error;
use tracing_subscriber::EnvFilter;
use crate::replication::PostgresStreamingError;

//
// #[tokio::main]
// async fn main_back() -> Result<()> {
//     if std::env::var("RUST_LOG").is_err() {
//         std::env::set_var("RUST_LOG", "logicaldecoding=info")
//     }
//     tracing_subscriber::fmt::fmt()
//         .with_env_filter(EnvFilter::from_default_env())
//         .init();
//
//     let m = Migrator::new(std::path::Path::new("./migrations")).await?;
//     let pool = PgPool::connect(&env::var("DATABASE_URL")?).await?;
//     m.run(&pool).await?;
//
//     let (replica_tx, replica_rx) = tokio::sync::oneshot::channel::<()>();
//     let (ransaction_tx, transaction_rx) = tokio::sync::broadcast::channel::<Transaction>(100);
//
//     let streaming_handle = task::spawn(async {
//       replication::postgres_worker("postgres", replica_tx, ransaction_tx).await
//     });
//
//     let clickhouse_handle = task::spawn(async {
//       replication::clickhouse_worker(transaction_rx).await
//     });
//
//     // block waiting for replication
//     replica_rx.await.unwrap();
//
//     streaming_handle.await.unwrap().unwrap();
//     clickhouse_handle.await.unwrap();
//
//     Ok(())
// }

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let ev = |name| {
        env::var(name).expect(&format!("env '{}' not found, please set '{}'", name, name))
    };
    let ev_def = |name, default| {
        env::var(name).unwrap_or(default)
    };

    let mut stopped = false;

    while !stopped {
        let db_user = ev("DB_USER");
        let db_pass = ev("DB_PASSWORD");
        let db_host = ev("DB_HOST");
        let db_port = ev_def("DB_PORT", "5432".into());
        let db_db = ev_def("DB_DATABASE", db_user.clone());

        let db_url = format!("postgres://{}:{}@{}:{}/{}", db_user, db_pass, db_host, db_port, db_db);
        let db_config = format!("user={} password={} host={} port={} dbname={} replication=database", db_user, db_pass, db_host, db_port, db_db);
        println!("pg-client to: {}", db_url);

        let tokio_future = replication::start_streaming_changes(db_config);
        if let Err(err) = tokio_future.await {
            error!("error: {:?}", err);

            match err {
                PostgresStreamingError::TokioPostgres(err) if err.is_closed() => {
                    stopped = false;
                },
                _ => stopped = true,
            };
        }
    };

    Ok(())
}
