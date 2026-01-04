use crate::connection::PostgresConnection;
use crate::engine::Load;
use flume::Sender;
use pin_utils::pin_mut;
use statistics::Event;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client,  Statement};
use tracing::{debug, error, info};
use util::container::Mapping;
use util::{TargetedMeta, container, DefinitionMapping, RelationalMapping, RelationalType};
use util::definition::{Definition, Entity, Stage};
use value::Value;

#[derive(Clone, Debug)]
pub struct Postgres {
    pub(crate) load: Arc<Mutex<Load>>,
    pub(crate) connector: PostgresConnection,
    pub(crate) client: Option<Arc<Client>>,
    pub(crate) prepared_statements: HashMap<(String, Stage), (Statement,Vec<Type>)>,
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

        Ok(())
    }


    pub(crate) async fn stop(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        container::stop("engine-postgres").await
    }

    pub(crate) fn cost(&self, _: &Value) -> f64 {
        1.0
    }

    pub(crate) async fn store(
        &self,
        stage: Stage,
        entity: String,
        values: Vec<(Value, TargetedMeta)>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => return Err(Box::from("Could not create postgres database")),
            Some(client) => {
                let rows_affected = self.copy_in(stage,client, entity, values).await?;
                //let rows_affected = self.load_insert(client, entity, values).await?;

                debug!("Inserted {} row(s) into 'users'.", rows_affected);
            }
        }
        Ok(())
    }

    pub async fn read(
        &self,
        entity: String,
        ids: Vec<u64>,
    ) -> Result<Vec<Value>, Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => Err(Box::from("Could not create postgres database")),
            Some(client) => {
                let insert_query = format!("SELECT * FROM {} WHERE id IN($1)", entity);
                let statement = client.prepare(&insert_query).await?;

                let res = client
                    .query(
                        &statement,
                        &[&ids.into_iter().map(Value::from).collect::<Vec<_>>()],
                    )
                    .await?
                    .into_iter()
                    .map(Value::from)
                    .collect::<Vec<_>>();
                Ok(res)
            }
        }
    }

    pub(crate) async fn monitor(
        &self,
        statistic_tx: &Sender<Event>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        loop {
            self.check_throughput(statistic_tx).await?;
            sleep(Duration::from_secs(5)).await;
        }
    }

    async fn check_throughput(
        &self,
        statistic_tx: &Sender<Event>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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

        statistic_tx
            .send_async(Event::EngineStatus(format!(
                "✅ Throughput (TPS): {:.2}",
                tps
            )))
            .await?;

        Ok(())
    }

    pub async fn init_entity(&mut self, definition: &Definition) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.create_table_plain(&definition.entity.plain).await?;

        if let DefinitionMapping::Relational(m) = &definition.mapping {
            self.create_table_mapped(m, definition.entity.clone()).await?;
        }

        Ok(())
    }

    pub async fn create_table_plain(&mut self, name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => return Err(Box::from("could not create postgres database")),
            Some(client) => {
                let create_table_query = format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                    _id SERIAL PRIMARY KEY,
                    id BIGINT,
                    value TEXT)",
                    name
                );

                client.execute(&create_table_query, &[]).await?;
                info!("Table '{}' ensured to exist.", name);
                let copy_query = format!("COPY {} (id, value) FROM STDIN BINARY", name);
                let statement = client.prepare(&copy_query).await?;
                self.prepared_statements.insert((name.to_string(), Stage::Plain), (statement, vec![Type::INT8, Type::TEXT]));
            }
        }
        Ok(())
    }

    async fn create_table_mapped(&mut self, mapping: &RelationalMapping, entity: Entity) -> Result<(), Box<dyn Error + Send + Sync>> {
        let types = mapping.get_types();

        match &self.client {
            None => return Err(Box::from("could not create postgres database")),
            Some(client) => {
                let create_table_query = format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                    _id SERIAL PRIMARY KEY,
                    {})",
                    entity.mapped,
                    types.iter().map(|(name, t)| format!("{} {}", name, t) ).collect::<Vec<_>>().join(",\n")
                );

                //info!("{}", create_table_query);

                client.execute(&create_table_query, &[]).await?;
                info!("Table '{}' ensured to exist.", entity.mapped);
                let copy_query = format!("COPY {} ({}) FROM STDIN BINARY", entity.mapped, types.iter().map(|(n, _)|n.to_string()).collect::<Vec<_>>().join(", "));

                let statement = client.prepare(&copy_query).await?;
                self.prepared_statements.insert((entity.mapped, Stage::Mapped), (statement, types.into_iter().map(|(_,t)| Self::pg_type(t)).collect()));
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
                let _ = client
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
        stage: Stage,
        client: &Arc<Client>,
        entity: String,
        values: Vec<(Value, TargetedMeta)>,
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let rows = values.len();

        let (query, types) = self.prepared_statements.get(&(entity, stage.clone())).unwrap();

        let sink = client
            .copy_in(query)
            .await?;

        let writer = BinaryCopyInWriter::new(sink, types);

        pin_mut!(writer);

        // 3. Encode each row
        for (value, meta) in values {
            match &stage {
                Stage::Plain => {
                    writer.as_mut().write(&[&(meta.id as i64), &serde_json::to_string(&value).unwrap()]).await?;
                }
                Stage::Mapped => {
                    if let Value::Array(a) = value {
                        writer.as_mut().write(&a.values.iter().map(|v| v as &(dyn ToSql + Sync)).collect::<Vec<_>>()).await?;
                    }else {
                        error!("error value {:?}", value)
                    }
                }
            }
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
        let (statement, _) = self
            .prepared_statements
            .get(&(entity.clone(), Stage::Plain))
            .ok_or(format!("No prepared statement for {} on postgres", entity))?;
        let mut affected_rows = 0;

        for value in values {
            affected_rows += client.execute(statement, &[&value]).await?
        }
        Ok(affected_rows as usize)
    }

    fn pg_type(t: RelationalType) -> Type {
        match t {
            RelationalType::Varchar(_) => Type::VARCHAR,
            RelationalType::Integer => Type::INT4,
            RelationalType::Float => Type::FLOAT4,
            RelationalType::Bool => Type::BOOL,
            RelationalType::Text => Type::TEXT,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::EngineKind;
    use tokio::task::JoinSet;
    use tracing_test::traced_test;
    use util::definition::{Entity, Stage};
    use util::{DefinitionMapping, Mapping, MappingSource, RelationalMapping, RelationalType, TargetedMeta};
    use value::Value;

    #[tokio::test]
    #[traced_test]
    pub async fn test_postgres() {
        let mut pg = EngineKind::postgres();
        let mut joins = JoinSet::new();
        pg.start(&mut joins).await.unwrap();

        pg.create_table_plain("users").await.unwrap();

        pg.store(
            Stage::Plain,
            String::from("users"),
            vec![(Value::text("test"), TargetedMeta::default())],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    #[traced_test]
    pub async fn test_postgres_mapped() {
        let mut pg = EngineKind::postgres();
        let mut joins = JoinSet::new();
        pg.start(&mut joins).await.unwrap();

        let r = RelationalMapping::Tuple(vec![("name".to_string(), RelationalType::Text),("age".to_string(), RelationalType::Integer)], Mapping{
            initial: MappingSource::List { keys: vec!["name".to_string(), "age".to_string()] },
            manual: vec![],
            auto: vec![]
        });

        pg.create_table_mapped(&r, Entity::new("users")).await.unwrap();

        pg.store(
            Stage::Mapped,
            String::from("users"),
            vec![(Value::array(vec![Value::text("test"), Value::int(30)]), TargetedMeta::default())],
        )
            .await
            .unwrap();
    }
}
