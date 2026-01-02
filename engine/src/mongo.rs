use crate::engine::Load;
use flume::Sender;
use futures_util::StreamExt;
use mongodb::bson::{Bson, doc};
use mongodb::options::{ClientOptions, ServerApi, ServerApiVersion};
use mongodb::{Client, Cursor};
use serde_json::json;
use statistics::Event;
use statistics::Event::EngineStatus;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tracing::{error, info};
use util::container::Mapping;
use util::{TargetedMeta, container, DefinitionMapping};
use util::definition::{Definition, Stage};
use value::Value;

#[derive(Clone, Debug)]
pub struct MongoDB {
    pub(crate) load: Arc<Mutex<Load>>,
    pub(crate) client: Option<Client>,
}

impl MongoDB {
    pub(crate) async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        container::start_container(
            "engine-mongodb",
            "mongo:latest",
            vec![Mapping {
                container: 27017,
                host: 27017,
            }],
            None,
        )
        .await?;

        let uri = format!("mongodb://localhost:{}", 27017);
        let mut client_options = ClientOptions::parse(uri).await?;

        let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
        client_options.server_api = Some(server_api);

        let client = Client::with_options(client_options)?;

        timeout(
            Duration::from_secs(5),
            client.database("admin").run_command(doc! { "ping": 1 }),
        )
        .await?
        .map_err(|err| format!("timeout after {}", err))?;
        info!("☑️ Connected to mongoDB database");

        self.client = Some(client);

        self.create_collection("_stream").await?;

        Ok(())
    }

    pub(crate) fn current_load(&self) -> Load {
        self.load.lock().unwrap().clone()
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
            None => Err(Box::from("No client")),
            Some(client) => {
                client
                    .database("public")
                    .collection(&entity)
                    .insert_many(values.into_iter().map(|(v, m)| {
                        Value::dict_from_pairs(vec![("value", v), ("id", Value::int(m.id as i64))])
                    }))
                    .bypass_document_validation(true)
                    .await?;

                Ok(())
            }
        }
    }

    pub async fn read(
        &self,
        entity: String,
        ids: Vec<u64>,
    ) -> Result<Vec<Value>, Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => Err(Box::from("No client")),
            Some(client) => {
                let mut res: Cursor<Value> = client
                    .database("public")
                    .collection(&entity)
                    .find(doc! {"id": {"$in": ids.into_iter().map(|id|Value::from(id)).collect::<Vec<_>>()}})
                    .await?;

                let mut values = vec![];

                while let Some(Ok(value)) = res.next().await {
                    values.push(value);
                }
                Ok(values)
            }
        }
    }

    pub(crate) async fn insert_data(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => Err(Box::from("No client")),
            Some(client) => {
                let user_document = doc! {
                    "name": "Alice Smith",
                    "age": 30,
                    "email": "alice.smith@example.com",
                    "hobbies": ["reading", "hiking", "coding"],
                    "address": {
                        "street": "123 Main St",
                        "city": "Anytown"
                    }
                };

                client
                    .database("public")
                    .collection("test")
                    .insert_one(user_document)
                    .await?;

                Ok(())
            }
        }
    }

    pub(crate) async fn monitor(
        &self,
        statistic_tx: &Sender<Event>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.create_collection(&definition.entity.plain).await?;

        if let DefinitionMapping::Document(_) = definition.mapping {
            self.create_collection(&definition.entity.mapped).await?;
        }

        Ok(())
    }

    pub(crate) async fn create_collection(
        &self,
        name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => Err(Box::from("No client")),
            Some(client) => {
                client.database("public").create_collection(name).await?;

                Ok(())
            }
        }
    }

    pub(crate) async fn stop(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        container::stop("engine-mongodb").await
    }

    async fn get_opcounters(&self) -> Result<HashMap<String, i64>, Box<dyn Error + Send + Sync>> {
        match &self.client {
            None => Err(Box::from("No client")),
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
