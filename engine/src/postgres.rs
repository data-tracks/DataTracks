use crate::connection::PostgresConnection;
use crate::engine::Load;
use pin_utils::pin_mut;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use flume::Sender;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Statement};
use tracing::{debug, info};
use statistics::Event;
use util::container;
use util::container::Mapping;
use value::Value;

#[derive(Clone, Debug)]
pub struct Postgres {
    pub(crate) load: Arc<Mutex<Load>>,
    pub(crate) connector: PostgresConnection,
    pub(crate) client: Option<Arc<Client>>,
    pub(crate) prepared_statements: HashMap<String, Statement>,
}

#[derive(Debug)]
struct TxCounts {
    commit: i64,
    rollback: i64,
}

impl Postgres {
    pub(crate) async fn start(
        &mut self,
        join: &mut JoinSet<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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

        let client = self.connector.connect(join).await?;
        info!("☑️ Connected to postgres database");
        timeout(Duration::from_secs(5), client.check_connection()).await??;
        self.client = Some(Arc::new(client));

        self.create_table("_stream").await?;

        Ok(())
    }

    pub(crate) fn current_load(&self) -> Load {
        self.load.lock().unwrap().clone()
    }

    pub(crate) async fn stop(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        container::stop("engine-postgres").await
    }

    pub(crate) fn cost(&self, _: &Value) -> f64 {
        1.0
    }

    pub(crate) async fn store(
        &self,
        entity: String,
        values: Vec<Value>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => return Err(Box::from("Could not create postgres database")),
            Some(client) => {
                let rows_affected = self.copy_in(client, entity, values).await?;
                //let rows_affected = self.load_insert(client, entity, values).await?;

                debug!("Inserted {} row(s) into 'users'.", rows_affected);
            }
        }
        Ok(())
    }

    pub(crate) async fn monitor(&self, statistic_tx: &Sender<Event>) -> Result<(), Box<dyn Error + Send + Sync>> {
        loop {
            self.check_throughput(statistic_tx).await?;
            sleep(Duration::from_secs(5)).await;
        }
    }

    async fn check_throughput(&self, statistic_tx: &Sender<Event>) -> Result<(), Box<dyn Error + Send + Sync>> {
        let interval_seconds = 5;

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

        statistic_tx.send_async(Event::Engine(format!("✅ Throughput (TPS): {:.2}", tps))).await?;

        Ok(())
    }

    pub async fn create_table(&mut self, name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
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
                let insert_query = format!("INSERT INTO {} (value) VALUES ($1)", name);
                let statement = client.prepare(&insert_query).await?;
                self.prepared_statements.insert(name.to_string(), statement);
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

    async fn copy_in(
        &self,
        client: &Arc<Client>,
        entity: String,
        values: Vec<Value>,
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let rows = values.len();

        let sink = client
            .copy_in(&format!("COPY {} (value) FROM STDIN BINARY", entity))
            .await?;

        let writer = BinaryCopyInWriter::new(sink, &[Type::TEXT]);

        pin_mut!(writer);

        // 3. Encode each row
        for value in values {
            writer.as_mut().write(&[&value]).await?;
        }

        writer.finish().await?;

        Ok(rows)
    }

    async fn load_insert(
        &self,
        client: &Arc<Client>,
        entity: String,
        values: Vec<Value>,
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let statement = self
            .prepared_statements
            .get(entity.as_str())
            .ok_or(format!("No prepared statement for {} on postgres", entity))?;
        let mut affected_rows = 0;

        for value in values {
            affected_rows += client.execute(statement, &[&value]).await?
        }
        Ok(affected_rows as usize)
    }
}

#[cfg(test)]
pub mod tests {
    use crate::EngineKind;
    use tokio::task::JoinSet;
    use value::Value;

    #[tokio::test]
    pub async fn test_postgres() {
        let mut pg = EngineKind::postgres();
        let mut joins = JoinSet::new();
        pg.start(&mut joins).await.unwrap();

        pg.create_table("users").await.unwrap();

        pg.store(String::from("users"), vec![Value::text("test")])
            .await
            .unwrap();
    }
}
