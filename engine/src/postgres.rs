use crate::connection::PostgresConnection;
use crate::engine::Load;
use anyhow::{anyhow, bail};
use flume::Sender;
use pin_utils::pin_mut;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout, Instant};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, Statement};
use tracing::{debug, info};
use processing::{Algebra, Schema};
use util::container::Mapping;
use util::definition::{Definition, Stage};
use util::{
    container, Batch, NativeMapping, EngineId, Event, PartitionId, RelationalMapping,
    RelationalType, TargetedRecord,
};
use value::Value;

pub(crate) static ID_BUILDER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct Postgres {
    pub(crate) id: Option<EngineId>,
    pub(crate) pg_id: u64,
    pub(crate) name: String,
    pub(crate) load: Arc<Mutex<Load>>,
    pub(crate) connector: PostgresConnection,
    pub(crate) client: Option<Arc<Client>>,
    pub(crate) prepared_statements: HashMap<(String, Stage), (Statement, Vec<Type>)>,
    pub(crate) join: Option<Arc<Mutex<JoinSet<()>>>>,
    pub(crate) deploy: bool,
}

impl Clone for Postgres {
    fn clone(&self) -> Self {
        Self {
            id: None,
            pg_id: ID_BUILDER.fetch_add(1, Ordering::Relaxed),
            name: self.name.clone(),
            load: Arc::new(Mutex::new(Load::Low)),
            connector: self.connector.clone(),
            client: None,
            prepared_statements: Default::default(),
            join: None,
            deploy: self.deploy,
        }
    }
}

impl<'de> Deserialize<'de> for Postgres {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawPostgres {
            host: String,
            port: u16,
            db: String,
            user: String,
            password: String,
            deploy: bool,
        }

        let raw = RawPostgres::deserialize(deserializer)?;

        let id = ID_BUILDER.fetch_add(1, Ordering::Relaxed);

        Ok(Postgres {
            id: None,
            pg_id: id,
            name: format!("postgres-{}", id),
            load: Arc::new(Mutex::new(Load::Low)),
            connector: PostgresConnection {
                url: raw.host.clone(),
                port: raw.port,
                db: raw.db.clone(),
                user: raw.user.clone(),
                password: raw.password.clone(),
            },
            client: None,
            prepared_statements: HashMap::new(),
            join: None,
            deploy: raw.deploy,
        })
    }
}

#[derive(Debug)]
struct TxCounts {
    commit: i64,
    rollback: i64,
}

impl Drop for Postgres {
    fn drop(&mut self) {
        if self.client.is_some() {
            info!("Dropping Postgres {:?}", self.id)
        }
    }
}

impl Postgres {
    pub(crate) async fn start<S: Into<EngineId>>(
        &mut self,
        join_set: &mut JoinSet<()>,
        id: S,
    ) -> anyhow::Result<()> {
        let client = self.connector.connect(join_set).await?;
        let id = id.into();
        debug!("☑️ Connected to Postgres database {}", id);
        self.id = Some(id);

        timeout(Duration::from_secs(5), client.check_connection()).await??;
        self.client = Some(Arc::new(client));
        self.join = Some(Arc::new(Mutex::new(JoinSet::new())));

        Ok(())
    }

    pub(crate) async fn start_container(&self) -> anyhow::Result<()> {
        if !self.deploy {
            return Ok(());
        }
        container::start_container(
            "engine-postgres",
            "postgres:latest",
            vec![Mapping {
                container: 5432,
                host: self.connector.port,
            }],
            Some(vec![format!("POSTGRES_PASSWORD={}", "postgres")]),
        )
        .await
    }

    pub(crate) async fn stop(&self) -> anyhow::Result<()> {
        container::stop("engine-postgres").await
    }

    pub(crate) fn cost(&self, _: &Value) -> f64 {
        1.0
    }

    pub(crate) async fn store(
        &self,
        stage: &Stage,
        entity: String,
        values: &Batch<TargetedRecord>,
    ) -> anyhow::Result<()> {
        let now = Instant::now();
        let len = values.len();

        match &self.client {
            None => bail!("Could not create postgres database"),
            Some(client) => {
                //let now = Instant::now();
                let rows_affected = self.copy_in(stage, client, &entity, values).await?;
                //let rows_affected = self.load_insert(client, entity, values).await?;

                //info!("duration {} {}", values.len(), now.elapsed().as_millis());
                debug!("Inserted {} row(s) into postgres engine.", rows_affected);
            }
        }
        debug!("inserted in postgres {} {:?}", len, now.elapsed());
        Ok(())
    }

    pub async fn read(&self, entity: String, ids: Vec<u64>) -> anyhow::Result<Vec<Value>> {
        match &self.client {
            None => bail!("Could not create postgres database"),
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

    pub(crate) async fn monitor(&self, statistic_tx: &Sender<Event>) -> anyhow::Result<()> {
        loop {
            self.check_throughput(statistic_tx).await?;
            sleep(Duration::from_secs(5)).await;
        }
    }

    async fn check_throughput(&self, statistic_tx: &Sender<Event>) -> anyhow::Result<()> {
        let interval_seconds = 5;

        // Initial read
        let start_counts = self.get_tx_counts().await.map_err(|err| anyhow!(err))?;
        sleep(Duration::from_secs(interval_seconds)).await;

        // Second read
        let end_counts = self.get_tx_counts().await.map_err(|err| anyhow!(err))?;

        // Calculation
        let total_tx = (end_counts.commit - start_counts.commit)
            + (end_counts.rollback - start_counts.rollback);
        let tps = total_tx as f64 / interval_seconds as f64;

        let load = match tps {
            t if t < 5.0 => Load::Low,
            t if t < 10.0 => Load::Middle,
            _ => Load::High,
        };

        *self.load.lock().map_err(|_| anyhow!("Poisoned lock"))? = load;

        statistic_tx
            .send_async(Event::EngineStatus(format!(
                "✅ Throughput (TPS): {:.2}",
                tps
            )))
            .await?;

        Ok(())
    }

    pub async fn init_entity(
        &mut self,
        definition: &Definition,
        partition_id: PartitionId,
        stage: &Stage,
    ) -> anyhow::Result<()> {
        if matches!(stage, Stage::Plain) {
            self.create_table_plain(&definition.entity_name(partition_id, &Stage::Plain))
                .await?;
        }

        if matches!(stage, Stage::Native)
            && let NativeMapping::Relational(m) = &definition.mapping
        {
            let name = definition.entity_name(partition_id, &Stage::Native);
            self.create_table_native(name.as_str(), m).await?;
        }

        if matches!(stage, Stage::Process)
            && let NativeMapping::Relational(_) = &definition.mapping
        {
            let name = definition.entity_name(partition_id, &Stage::Process);
            self.create_table_process(name.as_str(),Algebra::from(definition.processing.clone()).schema()).await?;
        }

        Ok(())
    }

    pub async fn create_table_plain(&mut self, name: &str) -> anyhow::Result<()> {
        match &self.client {
            None => bail!("could not create postgres database"),
            Some(client) => {
                let create_table_query = format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                    _id SERIAL PRIMARY KEY,
                    id BIGINT,
                    value BYTEA)",
                    name
                );

                client.execute(&create_table_query, &[]).await?;
                debug!(
                    "Table '{}' ensured to exist on {:?} pg_id {}.",
                    name, self.id, self.pg_id
                );

                let copy_query = format!("COPY {} (id, value) FROM STDIN BINARY", name);
                let statement = client.prepare(&copy_query).await?;
                self.prepared_statements.insert(
                    (name.to_string(), Stage::Plain),
                    (statement, vec![Type::INT8, Type::BYTEA]),
                );
            }
        }
        Ok(())
    }

    async fn create_table_native(
        &mut self,
        name: &str,
        mapping: &RelationalMapping,
    ) -> anyhow::Result<()> {
        let types = mapping.get_types();

        match &self.client {
            None => bail!("Could not create postgres database"),
            Some(client) => {
                let create_table_query = format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                    _id SERIAL PRIMARY KEY,
                    {})",
                    name,
                    types
                        .iter()
                        .map(|(name, t)| format!("{} {}", name, t))
                        .collect::<Vec<_>>()
                        .join(",\n")
                );

                //info!("{}", create_table_query);

                client.execute(&create_table_query, &[]).await?;
                //info!("Table '{}' ensured to exist.", name);
                let copy_query = format!(
                    "COPY {} ({}) FROM STDIN BINARY",
                    name,
                    types
                        .iter()
                        .map(|(n, _)| n.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );

                let statement = client.prepare(&copy_query).await?;
                self.prepared_statements.insert(
                    (name.to_string(), Stage::Native),
                    (
                        statement,
                        types.into_iter().map(|(_, t)| Self::pg_type(t)).collect(),
                    ),
                );
            }
        }

        Ok(())
    }

    async fn create_table_process(
        &mut self,
        name: &str,
        schema: Schema,
    ) -> anyhow::Result<()> {
        let types: Vec<_> = if let Schema::Fixed(types) = schema {
            types.into_iter().map(|(n, t)| (n, RelationalType::from(&t))).collect()
        }else {
            todo!()
        };

        match &self.client {
            None => bail!("Could not create postgres database"),
            Some(client) => {
                let create_table_query = format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                    _id SERIAL PRIMARY KEY,
                    {})",
                    name,
                    types
                        .iter()
                        .map(|(name, t)| format!("{} {}", name, t))
                        .collect::<Vec<_>>()
                        .join(",\n")
                );

                //info!("{}", create_table_query);

                client.execute(&create_table_query, &[]).await?;
                //info!("Table '{}' ensured to exist.", name);
                let copy_query = format!(
                    "COPY {} ({}) FROM STDIN BINARY",
                    name,
                    types
                        .iter()
                        .map(|(n, _)| n.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );

                let statement = client.prepare(&copy_query).await?;
                self.prepared_statements.insert(
                    (name.to_string(), Stage::Process),
                    (
                        statement,
                        types.into_iter().map(|(_, t)| Self::pg_type(t)).collect(),
                    ),
                );
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
        stage: &Stage,
        client: &Arc<Client>,
        entity: &String,
        values: &Batch<TargetedRecord>,
    ) -> anyhow::Result<usize> {
        let (query, types) = self
            .prepared_statements
            .get(&(entity.to_string(), stage.clone()))
            .ok_or(anyhow!("Statement not found"))?;

        let sink = client.copy_in(query).await?;
        let writer = BinaryCopyInWriter::new(sink, types);
        pin_mut!(writer);

        for chunk in values.chunks(1000) {
            for TargetedRecord { value, meta } in chunk {
                match stage {
                    Stage::Plain => {
                        let id_val = meta.id as i64;
                        //let row: [&(dyn ToSql + Sync); 2] = [&id_val, value];

                        writer
                            .as_mut()
                            .write(&[&id_val as &(dyn ToSql + Sync), value])
                            .await?;
                    }
                    Stage::Native => {
                        if let Value::Array(a) = value {
                            let row_params: Vec<&(dyn ToSql + Sync)> = a
                                .values
                                .par_iter()
                                .map(|v| v as &(dyn ToSql + Sync))
                                .collect();

                            writer.as_mut().write(&row_params).await?;
                        } else {
                            bail!("Expected Array value for Mapped stage, got {:?}", value);
                        }
                    }
                    Stage::Process => {
                        if let Value::Array(a) = value {
                            let row_params: Vec<&(dyn ToSql + Sync)> = a
                                .values
                                .par_iter()
                                .map(|v| v as &(dyn ToSql + Sync))
                                .collect();

                            writer.as_mut().write(&row_params).await?;
                        } else {
                            bail!("Expected Array value for Mapped stage, got {:?}", value);
                        }
                    }
                    _ => panic!(),
                }
            }
        }
        writer.finish().await?;
        Ok(values.len())
    }

    #[allow(dead_code)]
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
    use util::definition::Stage;
    use util::{
        batch, target, Mapping, MappingSource, RelationalMapping, RelationalType, TargetedMeta,
    };
    use value::Value;

    #[tokio::test]
    #[traced_test]
    pub async fn test_postgres() {
        let mut pg = EngineKind::postgres();
        let mut join_set = JoinSet::new();
        pg.start_container().await.unwrap();
        pg.start(&mut join_set, 0).await.unwrap();

        pg.create_table_plain("users").await.unwrap();

        pg.store(
            &Stage::Plain,
            String::from("users"),
            &batch![target!(Value::text("test"), TargetedMeta::default())],
        )
        .await
        .unwrap();
        pg.stop().await.unwrap();
    }

    //#[tokio::test]
    //#[traced_test]
    pub async fn test_postgres_mapped() {
        let mut pg = EngineKind::postgres_with_port(5433);
        pg.start_container().await.unwrap();
        let mut join_set = JoinSet::new();
        pg.start(&mut join_set, 0).await.unwrap();

        let r = RelationalMapping::Tuple(
            vec![
                ("name".to_string(), RelationalType::Text),
                ("age".to_string(), RelationalType::Integer),
            ],
            Mapping {
                initial: MappingSource::List {
                    keys: vec!["name".to_string(), "age".to_string()],
                },
                manual: vec![],
                auto: vec![],
            },
        );

        pg.create_table_native("users", &r).await.unwrap();

        pg.store(
            &Stage::Native,
            String::from("users"),
            &batch![target!(
                Value::array(vec![Value::text("test"), Value::int(30)]),
                TargetedMeta::default()
            )],
        )
        .await
        .unwrap();

        pg.stop().await.unwrap();
    }
}
