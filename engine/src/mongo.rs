use crate::{engine, Engine};
use mongodb::bson::doc;
use mongodb::options::{ClientOptions, ServerApi, ServerApiVersion};
use mongodb::Client;
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tracing::info;
use util::container;
use util::container::Mapping;
use crate::neo::Neo4j;

#[derive(Clone)]
pub struct MongoDB {
    pub(crate) client: Option<Client>,
}

impl Into<Engine> for MongoDB {
    fn into(self) -> Engine {
        Engine::MongoDB(self)
    }
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
        // Set the server_api field of the client_options object to Stable API version 1
        let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
        client_options.server_api = Some(server_api);
        // Create a new client and connect to the server
        let client = mongodb::Client::with_options(client_options)?;
        // Send a ping to confirm a successful connection
        timeout(
            Duration::from_secs(5),
            client.database("admin").run_command(doc! { "ping": 1 }),
        )
        .await
        .map(|res| ())
        .map_err(|err| format!("timeout after {}", err))?;
        info!("☑️ Connected to mongoDB database");

        self.client = Some(client);

        self.measure_opcounters().await?;
        Ok(())
    }

    pub(crate) async fn monitor(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.measure_opcounters().await
    }

    pub(crate) async fn stop(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
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

    async fn measure_opcounters(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let interval_seconds = 5.0;
        let start_counts = self.get_opcounters().await?;
        sleep(Duration::from_secs_f64(interval_seconds)).await;

        // Second read
        let end_counts = self.get_opcounters().await?;

        let mut text = "✅ Metrics (Ops/Sec):".to_string();

        // Calculate and print the rate for each counter
        for (op_type, end_count) in end_counts {
            if let Some(start_count) = start_counts.get(&op_type) {
                let diff = end_count - start_count;
                let rate = diff as f64 / interval_seconds;
                text += &format!(", {}: {:.2}", op_type, rate);
            }
        }
        info!("{}", text);

        Ok(())
    }
}
