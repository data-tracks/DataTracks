use crate::engine::Load;
use anyhow::{bail, Context};
use flume::Sender;
use futures_util::StreamExt;
use mongodb::bson::doc;
use mongodb::options::{ClientOptions, ServerApi, ServerApiVersion};
use mongodb::{Client, Cursor};
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};
use tracing::{debug, error, info};
use util::container::Mapping;
use util::definition::{Definition, Stage};
use util::Event::EngineStatus;
use util::{container, Batch, NativeMapping, EngineId, Event, PartitionId, TargetedRecord};
use value::Value;

#[derive(Debug, Default)]
pub struct MongoDB {
    pub(crate) id: Option<EngineId>,
    pub(crate) load: Arc<Mutex<Load>>,
    pub(crate) client: Option<Client>,
    pub(crate) host: String,
    pub(crate) port: u32,
    pub names: HashMap<(String, Stage), String>,
    pub deploy: bool,
}

impl<'de> Deserialize<'de> for MongoDB {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            host: String,
            port: u32,
            deploy: bool,
        }

        let raw = Raw::deserialize(deserializer)?;

        Ok(MongoDB {
            id: None,
            load: Arc::new(Mutex::new(Load::Low)),
            client: None,
            host: raw.host.clone(),
            port: raw.port,
            names: Default::default(),
            deploy: raw.deploy,
        })
    }
}

impl Clone for MongoDB {
    fn clone(&self) -> Self {
        Self {
            id: None,
            port: self.port,
            load: Arc::new(Mutex::new(Load::Low)),
            client: None,
            names: Default::default(),
            host: self.host.clone(),
            deploy: false,
        }
    }
}

impl Drop for MongoDB {
    fn drop(&mut self) {
        if self.client.is_some() {
            info!("Dropping MongoDB {:?}", self.id)
        }
    }
}

impl MongoDB {
    pub(crate) async fn start<S: Into<EngineId>>(&mut self, id: S) -> anyhow::Result<()> {
        let uri = format!("mongodb://{}:{}", self.host, self.port);
        let mut client_options = ClientOptions::parse(uri).await?;

        let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
        client_options.server_api = Some(server_api);
        client_options.max_connecting = Some(24);
        client_options.max_pool_size = Some(24);
        client_options.server_selection_timeout = Some(Duration::from_secs(60));

        let client = Client::with_options(client_options)?;

        timeout(
            Duration::from_secs(10),
            client.database("admin").run_command(doc! { "ping": 1 }),
        )
        .await??;
        let id = id.into();
        debug!("☑️ Connected to MongoDB database {}", id);
        self.id = Some(id);

        self.client = Some(client);

        Ok(())
    }

    pub(crate) async fn start_container(&self) -> anyhow::Result<()> {
        if !self.deploy {
            return Ok(());
        }
        container::start_container(
            "engine-mongodb",
            "mongo:latest",
            vec![Mapping {
                container: 27017,
                host: 27017,
            }],
            None,
        )
        .await
    }

    pub(crate) fn cost(&self, _: &Value) -> f64 {
        1.0
    }

    pub(crate) async fn store(
        &self,
        _: &Stage,
        entity: String,
        values: &Batch<TargetedRecord>,
    ) -> anyhow::Result<()> {
        let now = Instant::now();
        let client = self.client.as_ref().context("No client")?;
        let collection = client
            .database("public")
            .collection::<mongodb::bson::Document>(&entity);

        for chunk in values.records.chunks(10_000) {
            let docs: Vec<mongodb::bson::Document> = chunk
                .iter()
                .map(|rec| {
                    doc! {
                        "value": &rec.value,
                        "id": rec.meta.id as i64,
                    }
                })
                .collect();

            // One chunk at a time keeps memory low and errors simple
            collection.insert_many(docs).ordered(false).await?;
        }

        debug!("Inserted 100k records in {:?}", now.elapsed());
        Ok(())
    }

    pub async fn read(&self, entity: String, ids: Vec<u64>) -> anyhow::Result<Vec<Value>> {
        match &self.client {
            None => bail!("No client"),
            Some(client) => {
                let mut res: Cursor<Value> = client
                    .database("public")
                    .collection(&entity)
                    .find(
                        doc! {"id": {"$in": ids.into_iter().map(Value::from).collect::<Vec<_>>()}},
                    )
                    .await?;

                let mut values = vec![];

                while let Some(Ok(value)) = res.next().await {
                    values.push(value);
                }
                Ok(values)
            }
        }
    }

    pub(crate) async fn monitor(&self, statistic_tx: &Sender<Event>) -> anyhow::Result<()> {
        loop {
            match self.measure_opcounters(statistic_tx).await {
                Ok(_) => {}
                Err(err) => {
                    error!("error during measure of mongo: {}", err)
                }
            };
            sleep(Duration::from_secs(5)).await;
        }
    }

    pub(crate) async fn init_entity(
        &self,
        definition: &Definition,
        partition_id: PartitionId,
        stage: &Stage,
    ) -> anyhow::Result<()> {
        if matches!(stage, Stage::Plain) {
            let name = definition.entity_name(partition_id, &Stage::Plain);
            self.create_collection(name.as_str()).await?;
        }

        if matches!(stage, Stage::Native)
            && let NativeMapping::Document(_) = definition.mapping
        {
            let name = definition.entity_name(partition_id, &Stage::Native);
            self.create_collection(name.as_str()).await?;
        }

        if matches!(stage, Stage::Process)
            && let NativeMapping::Document(_) = definition.mapping
        {
            let name = definition.entity_name(partition_id, &Stage::Process);
            self.create_collection(name.as_str()).await?;
        }

        Ok(())
    }

    pub(crate) async fn create_collection(&self, name: &str) -> anyhow::Result<()> {
        match &self.client {
            None => bail!("No client"),
            Some(client) => {
                client.database("public").create_collection(name).await?;

                Ok(())
            }
        }
    }

    pub(crate) async fn stop(&self) -> anyhow::Result<()> {
        container::stop("engine-mongodb").await
    }

    async fn get_opcounters(&self) -> anyhow::Result<HashMap<String, i64>> {
        match &self.client {
            None => bail!("No client"),
            Some(client) => {
                // Run the db.serverStatus() command
                let status_doc = client
                    .database("admin")
                    .run_command(doc! { "serverStatus": 1 })
                    .await?;

                // Extract the opcounters section
                let opcounters = status_doc
                    .get_document("opcounters")
                    .map_err(|_| mongodb::error::Error::custom("opcounters field missing"))?;

                let mut counters = HashMap::new();
                for (key, value) in opcounters {
                    if let Some(count) = value.as_i64() {
                        counters.insert(key.to_string(), count);
                    }
                }
                Ok(counters)
            }
        }
    }

    async fn measure_opcounters(
        &self,
        statistic_tx: &Sender<Event>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let interval_seconds = 5.0;
        let start_counts = self.get_opcounters().await?;
        sleep(Duration::from_secs_f64(interval_seconds)).await;

        // Second read
        let end_counts = self.get_opcounters().await?;

        let mut text = "✅ Metrics (Ops/Sec):".to_string();

        let mut insert_ops = 1.0;

        // Calculate and print the rate for each counter
        for (op_type, end_count) in end_counts {
            if let Some(start_count) = start_counts.get(&op_type) {
                let diff = end_count - start_count;
                let rate = diff as f64 / interval_seconds;
                text += &format!(", {}: {:.2}", op_type, rate);

                if op_type == "insert" {
                    insert_ops = rate;
                }
            }
        }

        let load = match insert_ops {
            t if t < 5.0 => Load::Low,
            t if t < 10.0 => Load::Middle,
            _ => Load::High,
        };

        *self.load.lock().unwrap() = load;

        statistic_tx.send_async(EngineStatus(text)).await?;

        Ok(())
    }
}
