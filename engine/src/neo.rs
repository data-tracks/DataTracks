use crate::{engine, Engine};
use neo4rs::{query, BoltType, Graph};
use reqwest::Client;
use serde::Deserialize;
use std::error::Error;
use std::time::Duration;
use serde::de::Unexpected::Str;
use tokio::spawn;
use tokio::time::{sleep, Instant};
use tracing::info;
use util::container;
use util::container::Mapping;
use value::{Float, Value};

#[derive(Clone)]
pub struct Neo4j {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) database: String,
    pub(crate) graph: Option<Graph>,
}

#[derive(Debug, Deserialize)]
struct TxMetrics {
    #[serde(rename = "neo4j.transaction.commits")]
    commits: f64,
    #[serde(rename = "neo4j.transaction.rollbacks")]
    rollbacks: f64,
}

impl Into<Engine> for Neo4j {
    fn into(self) -> Engine {
        Engine::Neo4j(self)
    }
}

impl Neo4j {
    pub(crate) async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        container::start_container(
            "engine-neo4j",
            "neo4j:latest",
            vec![
                Mapping {
                    container: 7687,
                    host: 7687,
                },
                Mapping {
                    container: 7474,
                    host: 7474,
                },
            ],
            Some(vec![format!("NEO4J_AUTH=neo4j/{}", "neoneoneo")]),
        )
        .await?;

        let uri = format!("{}:{}", self.host, self.port);

        let graph = Graph::new(&uri, self.user.clone(), self.password.clone())?;

        let start_time = Instant::now();

        loop {
            match graph.run("MATCH (n) RETURN n").await {
                Ok(_) => break,
                Err(e) => {
                    let time = Instant::now();
                    if time.duration_since(start_time).as_secs() > 60 {
                        return Err(Box::new(e));
                    }
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }

        info!("️️☑️ Connected to neo4j");
        self.graph = Some(graph);

        self.check_throughput().await?;
        Ok(())
    }

    pub(crate) async fn stop(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        container::stop("engine-neo4j").await
    }

    pub(crate) fn cost(&self, value: &Value) -> f64 {
        0.0
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

    pub(crate) async fn insert_data(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.graph {
            None => Err(Box::from("No graph")),
            Some(g) => {
                let cypher_query = "
                    CREATE (p:Person {name: $name, age: $age, is_active: $active})
                    RETURN p
                ";

                match g
                    .run(
                        query(cypher_query)
                            .param("name", "Jane Doe")
                            .param("age", 25)
                            .param("active", true),
                    )
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => Err(Box::new(e)),
                }
            }
        }
    }

    pub(crate) async fn store(&self, value: Value) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.graph {
            None => Err(Box::from("No graph")),
            Some(g) => {
                let cypher_query = self.query(&value);

                match g
                    .run(
                        query(&cypher_query)
                            .param("value", value),
                    )
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => Err(Box::new(e)),
                }
            }
        }
    }

    async fn check_throughput(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let http_client = Client::new();
        let management_uri = "http://localhost:7474";

        let interval_seconds = 5.0;
        let url = format!("{}/db/neo4j/management/metrics/json", management_uri); // Example endpoint

        // Function to fetch metrics (simplified for a hypothetical JSON endpoint)
        let fetch_metrics = |client: Client, url: String| async move {
            let resp = client
                .get(url)
                .basic_auth(self.user.clone(), Some(self.password.clone()))
                .send()
                .await?;
            let json_map: serde_json::Value = resp.json().await?;
            // Manually navigate the map to extract required values
            Ok::<TxMetrics, Box<dyn Error + Send + Sync>>(TxMetrics {
                commits: json_map["neo4j.transaction.commits"]["count"]
                    .as_f64()
                    .unwrap_or(0.0),
                rollbacks: json_map["neo4j.transaction.rollbacks"]["count"]
                    .as_f64()
                    .unwrap_or(0.0),
            })
        };

        // Read 1
        let start = fetch_metrics(http_client.clone(), url.clone()).await?;
        sleep(Duration::from_secs_f64(interval_seconds)).await;

        // Read 2
        let end = fetch_metrics(http_client, url).await?;

        // Calculate TPS
        let total_tx = (end.commits - start.commits) + (end.rollbacks - start.rollbacks);
        let tps = total_tx / interval_seconds;

        info!("✅ Throughput (TPS): {:.2}", tps);

        Ok(())
    }

    fn query(&self, value: &Value) -> String {
        String::from("CREATE (p:Person {value: $value}) RETURN p")
    }
}

