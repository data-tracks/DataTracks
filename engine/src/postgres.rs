use crate::connection::PostgresConnection;
use crate::engine;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_postgres::{Client, GenericClient};
use tracing::info;
use util::container;
use util::container::{Manager, Mapping};

#[derive(Clone)]
pub struct Postgres {
    pub(crate) connector: PostgresConnection,
    pub(crate) client: Option<Arc<Client>>,
}

#[derive(Debug)]
struct TxCounts {
    commit: i64,
    rollback: i64,
}

impl Postgres {
    pub(crate) async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        container::start_container(
            "engine-postgres",
            "postgres:latest",
            vec![Mapping {
                container: 5432,
                host: 5432,
            }],
            Some(vec![format!("POSTGRES_PASSWORD={}", "postgres")]),
        )
        .await?;

        let client = self.connector.connect().await?;
        info!("☑️ Connected to postgres database");
        timeout(Duration::from_secs(5), client.check_connection()).await??;
        self.client = Some(Arc::new(client));

        self.check_throughput().await?;

        Ok(())
    }

    pub(crate) async fn stop(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        container::stop("engine-postgres").await
    }

    pub(crate) async fn monitor(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.check_throughput().await
    }

    async fn check_throughput(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let interval_seconds = 5;
        info!("--- Monitoring TPS over {} seconds ---", interval_seconds);

        // Initial read
        let start_counts = self.get_tx_counts().await?;
        sleep(Duration::from_secs(interval_seconds)).await;

        // Second read
        let end_counts = self.get_tx_counts().await?;

        // Calculation
        let total_tx = (end_counts.commit - start_counts.commit)
            + (end_counts.rollback - start_counts.rollback);
        let tps = total_tx as f64 / interval_seconds as f64;

        info!("✅ Throughput (TPS): {:.2}", tps);

        Ok(())
    }

    async fn get_tx_counts(&self) -> Result<TxCounts, Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => {}
            Some(client) => {
                let row = client.query_one(
                    "SELECT xact_commit, xact_rollback FROM pg_stat_database WHERE datname = current_database()",
                    &[],
                ).await?;

                return Ok(TxCounts {
                    commit: row.get(0),
                    rollback: row.get(1),
                });
            }
        }

        Err(Box::from("client not found"))
    }
}
