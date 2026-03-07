use crate::engine::Load;
use anyhow::bail;
use flume::Sender;
use futures_util::StreamExt;
use mongodb::bson::doc;
use mongodb::options::{ClientOptions, ServerApi, ServerApiVersion};
use mongodb::{Client, Cursor};
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use rayon::iter::ParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use tokio::time::{sleep, timeout, Instant};
use tracing::{debug, error, info};
use util::Event::EngineStatus;
use util::container::Mapping;
use util::definition::{Definition, Stage};
use util::{Batch, DefinitionMapping, EngineId, Event, PartitionId, TargetedRecord, container};
use value::Value;

#[derive(Debug)]
pub struct MongoDB {
    pub(crate) id: Option<EngineId>,
    pub(crate) load: Arc<Mutex<Load>>,
    pub(crate) client: Option<Client>,
    pub names: HashMap<(String, Stage), String>,
}

impl Clone for MongoDB {
    fn clone(&self) -> Self {
        Self {
            id: None,
            load: Arc::new(Mutex::new(Load::Low)),
            client: None,
            names: Default::default(),
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
    pub(crate) async fn start<S: Into<EngineId>>(
        &mut self,
        id: S,
    ) -> anyhow::Result<()> {
        let uri = format!("mongodb://localhost:{}", 27017);
        let mut client_options = ClientOptions::parse(uri).await?;

        let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
        client_options.server_api = Some(server_api);
        client_options.max_connecting = Some(32);
        client_options.max_pool_size = Some(32);

        let client = Client::with_options(client_options)?;

        timeout(
            Duration::from_secs(5),
            client.database("admin").run_command(doc! { "ping": 1 }),
        )
        .await??;
        let id = id.into();
        info!("☑️ Connected to MongoDB database {}", id);
        self.id = Some(id);

        self.client = Some(client);

        Ok(())
    }

    pub(crate) async fn start_container(&self) -> anyhow::Result<()> {
        container::start_container(
            "engine-mongodb",
            "mongo:latest",
            vec![Mapping {
                container: 27017,
                host: 27017,
            }],
            None,
        ).await
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
        let len = values.len();
        match &self.client {
            None => bail!("No client"),
            Some(client) => {
                //let now = Instant::now();
                client
                    .database("public")
                    .collection(&entity)
                    .insert_many(values.records.par_iter().map(|TargetedRecord { value, meta }| {
                        Value::dict_from_pairs(vec![
                            ("value", value.clone()),
                            ("id", Value::int(meta.id as i64)),
                        ])
                    }).collect::<Vec<_>>())
                    .ordered(false)
                    .bypass_document_validation(true)
                    .await?;

                debug!("inserted in mongo {} {:?}", len, now.elapsed());
                Ok(())
            }
        }
    }

    pub async fn read(
        &self,
        entity: String,
        ids: Vec<u64>,
    ) -> anyhow::Result<Vec<Value>> {
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


        if matches!(stage, Stage::Mapped) && let DefinitionMapping::Document(_) = definition.mapping {
            let name = definition.entity_name(partition_id, &Stage::Mapped);
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
