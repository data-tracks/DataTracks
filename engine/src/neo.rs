use crate::engine::Load;
use crate::EngineKind;
use futures_util::future::join_all;
use neo4rs::{query, Graph, RunResult};
use reqwest::Client;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::f32::consts::E;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::spawn;
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};
use tracing::{error, info};
use util::container;
use util::container::Mapping;
use value::Value;

#[derive(Clone)]
pub struct Neo4j {
    pub(crate) load: Arc<Mutex<Load>>,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) database: String,
    pub(crate) graph: Option<Graph>,
    pub(crate) prepared_queries: HashMap<String, String>,
}

impl Debug for Neo4j {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Neo4j")
    }
}

#[derive(Debug, Deserialize)]
struct TxMetrics {
    #[serde(rename = "neo4j.transaction.commits")]
    commits: f64,
    #[serde(rename = "neo4j.transaction.rollbacks")]
    rollbacks: f64,
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

    pub(crate) async fn create_entity(&mut self, name: &str) {
        let cypher_query = self.query(String::from(name));
        self.prepared_queries
            .insert(String::from(name), cypher_query);
    }

    pub(crate) async fn stop(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        container::stop("engine-neo4j").await
    }

    pub(crate) fn current_load(&self) -> Load {
        self.load.lock().unwrap().clone()
    }

    pub(crate) fn cost(&self, _: &Value) -> f64 {
        1.0
    }

    pub(crate) async fn monitor(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let clone = self.clone();

        loop {
            clone.check_throughput().await?;
            sleep(Duration::from_secs(5)).await;
        }
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

    pub(crate) async fn store(
        &self,
        entity: String,
        values: Vec<Value>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.graph {
            None => Err(Box::from("No graph")),
            Some(g) => {
                let cypher_query = self
                    .prepared_queries
                    .get(&entity)
                    .ok_or(format!("No prepared query in neo4j for {}", entity))?;

                let tx = g.start_txn().await?;
                let mut waits = vec![];
                for v in values {
                    waits.push(g.run(query(cypher_query).param("value", v)));
                }

                let errors: Vec<String> = join_all(waits)
                    .await
                    .into_iter()
                    .filter_map(|r| match r {
                        Ok(_) => None,
                        Err(err) => Some(err.to_string()),
                    })
                    .collect();

                if errors.is_empty() {
                    tx.commit().await?;
                    Ok(())
                } else {
                    Err(Box::from(errors.first().unwrap().to_string()))
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

        let load = match tps {
            t if t < 5.0 => Load::Low,
            t if t < 10.0 => Load::Middle,
            _ => Load::High,
        };

        *self.load.lock().unwrap() = load;

        info!("✅ Throughput (TPS): {:.2}", tps);

        Ok(())
    }

    fn query(&self, entity: String) -> String {
        format!("CREATE (p:db_{} {{value: $value}}) RETURN p", entity)
    }
}
