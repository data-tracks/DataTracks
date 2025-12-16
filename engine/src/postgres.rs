use crate::connection::PostgresConnection;
use crate::engine::Load;
use crate::neo::Neo4j;
use crate::{engine, Engine};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::spawn;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_postgres::{Client, GenericClient, SimpleQueryMessage};
use tracing::{debug, info};
use util::container;
use util::container::{Manager, Mapping};
use util::queue::{Meta, RecordQueue};
use value::{Float, Value};

#[derive(Clone)]
pub struct Postgres {
    pub(crate) queue: RecordQueue,
    pub(crate) load: Arc<Mutex<Load>>,
    pub(crate) connector: PostgresConnection,
    pub(crate) client: Option<Arc<Client>>,
}

#[derive(Debug)]
struct TxCounts {
    commit: i64,
    rollback: i64,
}

impl Into<Engine> for Postgres {
    fn into(self) -> Engine {
        Engine::Postgres(self, RecordQueue::new())
    }
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

        self.create_table("_stream").await?;

        Ok(())
    }

    pub(crate) fn current_load(&self) -> Load {
        self.load.lock().unwrap().clone()
    }

    pub(crate) async fn stop(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        container::stop("engine-postgres").await
    }

    pub(crate) fn cost(&self, _: &Value) -> f64 {
        1.0
    }

    pub(crate) async fn store(
        &self,
        value: Value,
        entity: String,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => return Err(Box::from("could not create postgres database")),
            Some(client) => {
                let insert_query = format!("INSERT INTO {} (value) VALUES ($1)", entity);
                let rows_affected = client.execute(&insert_query, &[&value]).await?;

                debug!("Inserted {} row(s) into 'users'.", rows_affected);
            }
        }
        Ok(())
    }

    pub(crate) async fn monitor(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let clone = self.clone();
        spawn(async move {
            loop {
                clone.check_throughput().await.unwrap();
                sleep(Duration::from_secs(5)).await;
            }
        });
        Ok(())
    }

    async fn check_throughput(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let interval_seconds = 5;
        //info!("--- Monitoring TPS over {} seconds ---", interval_seconds);

        // Initial read
        let start_counts = self.get_tx_counts().await?;
        sleep(Duration::from_secs(interval_seconds)).await;

        // Second read
        let end_counts = self.get_tx_counts().await?;

        // Calculation
        let total_tx = (end_counts.commit - start_counts.commit)
            + (end_counts.rollback - start_counts.rollback);
        let tps = total_tx as f64 / interval_seconds as f64;

        let load = match tps {
            t if t < 5.0 => Load::Low,
            t if t < 10.0 => Load::Middle,
            _ => Load::High,
        };

        *self.load.lock().unwrap() = load;

        info!("✅ Throughput (TPS): {:.2}", tps);

        Ok(())
    }

    pub async fn create_table(&self, name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => return Err(Box::from("could not create postgres database")),
            Some(client) => {
                let create_table_query = format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    value TEXT)",
                    name
                );

                client.execute(&create_table_query, &[]).await?;
                info!("Table '{}' ensured to exist.", name);
            }
        }
        Ok(())
    }

    pub async fn insert_data(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => return Err(Box::from("could not create postgres database")),
            Some(client) => {
                let user_name = "Alice";
                let user_age = 30;

                let insert_query = "INSERT INTO users (name, age) VALUES ($1, $2)";
                let rows_affected = client
                    .execute(insert_query, &[&user_name, &user_age])
                    .await?;

                //info!("Inserted {} row(s) into 'users'.", rows_affected);
            }
        }
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
