use crate::engine::Load;
use anyhow::bail;
use flume::Sender;
use neo4rs::{Graph, query};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::{debug, info};
use util::Event::EngineStatus;
use util::container::Mapping;
use util::definition::{Definition, Stage};
use util::{DefinitionMapping, Event, TargetedRecord, container, Batch};
use value::Value;

#[derive(Clone)]
pub struct Neo4j {
    pub(crate) name: String,
    pub(crate) load: Arc<Mutex<Load>>,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) graph: Option<Graph>,
    pub(crate) prepared_queries: HashMap<(Stage, String), String>,
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
    pub(crate) async fn start(&mut self) -> anyhow::Result<()> {
        container::start_container(
            self.name.as_str(),
            "neo4j:latest",
            vec![
                Mapping {
                    container: 7687,
                    host: self.port,
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
                        bail!(e);
                    }
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }

        info!("️️☑️ Connected to neo4j");
        self.graph = Some(graph);

        Ok(())
    }

    pub(crate) async fn init_entity(&mut self, definition: &Definition) {
        // native query
        let cypher_query = self.create_value_query(&definition.entity.plain);
        self.prepared_queries.insert(
            (Stage::Plain, definition.entity.plain.clone()),
            cypher_query,
        );

        if let DefinitionMapping::Graph(_) = definition.mapping {
            // mapped query
            let cypher_query = self.create_node_query(&definition.entity.mapped);
            self.prepared_queries.insert(
                (Stage::Mapped, definition.entity.mapped.clone()),
                cypher_query,
            );
        }
    }

    pub(crate) async fn stop(&self) -> anyhow::Result<()> {
        container::stop(self.name.as_str()).await
    }

    pub(crate) fn cost(&self, _: &Value) -> f64 {
        1.0
    }

    pub(crate) async fn monitor(
        &self,
        statistic_tx: &Sender<Event>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let clone = self.clone();

        loop {
            clone.check_throughput(statistic_tx).await?;
            sleep(Duration::from_secs(5)).await;
        }
    }

    pub(crate) async fn store(
        &self,
        stage: Stage,
        entity: String,
        values: &Batch<TargetedRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.graph {
            None => Err(Box::from("No graph")),
            Some(g) => {
                let len = values.len();

                let cypher_query = self
                    .prepared_queries
                    .get(&(stage.clone(), entity.clone()))
                    .ok_or(format!("No prepared query in neo4j for {}", entity))?;

                let values = match &stage {
                    Stage::Plain => Self::wrap_value_plain(values),
                    Stage::Mapped => Self::wrap_value_mapped(values),
                };

                g.run(query(cypher_query).param("values", values)).await?;

                debug!("neo4j values {}", len);
                Ok(())
            }
        }
    }

    fn wrap_value_plain(values: &Batch<TargetedRecord>) -> Vec<Vec<Value>> {
        values
            .into_iter()
            .map(|TargetedRecord { value, meta }| {
                vec![
                    Value::text(&serde_json::to_string(&value).unwrap()),
                    Value::int(meta.id as i64),
                ]
            })
            .collect::<Vec<_>>()
    }

    fn wrap_value_mapped(values: &Batch<TargetedRecord>) -> Vec<Vec<Value>> {
        values
            .into_iter()
            .map(|TargetedRecord { value, meta }| vec![value.clone(), Value::int(meta.id as i64)])
            .collect::<Vec<_>>()
    }

    pub async fn read(
        &self,
        entity: String,
        ids: Vec<u64>,
    ) -> Result<Vec<Value>, Box<dyn Error + Send + Sync>> {
        match &self.graph {
            None => Err(Box::from("No graph")),
            Some(g) => {
                let len = ids.len();

                let cypher_query = self
                    .prepared_queries
                    .get(&(Stage::Plain, entity.clone()))
                    .ok_or(format!("No prepared query in neo4j for {}", entity))?;

                let mut res = g
                    .execute_read(
                        query(cypher_query).param(
                            "values",
                            ids.into_iter()
                                .map(Value::from)
                                .collect::<Vec<_>>(),
                        ),
                    )
                    .await?;

                debug!("neo4j values {}", len);
                let mut values = vec![];
                while let Some(value) = res.next().await? {
                    values.push(Value::from(value))
                }
                Ok(values)
            }
        }
    }

    async fn check_throughput(
        &self,
        statistic_tx: &Sender<Event>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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

        *self.load.lock().map_err(|err| err.to_string())? = load;

        statistic_tx
            .send_async(EngineStatus(format!("✅ Throughput (TPS): {:.2}", tps)))
            .await?;

        Ok(())
    }

    fn create_value_query(&self, entity: &str) -> String {
        format!(
            "UNWIND $values as row \
            CREATE (p:db_{} {{value: row[0], id: row[1]}})",
            entity
        )
    }

    fn create_node_query(&self, entity: &str) -> String {
        format!(
            "UNWIND $values AS row CREATE (n:db_{}:$(row[0].labels)) SET n = row[0].props SET n._id = row[1]",
            entity
        )
    }

    #[allow(dead_code)]
    fn read_query(&self, entity: String) -> String {
        format!(
            "MATCH (p:db_{}) \
            WHERE p.id IN $values",
            entity
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::EngineKind;
    use neo4rs::{BoltInteger, BoltMap, BoltString, BoltType, query};
    use std::collections::{BTreeMap, HashMap};
    use std::vec;
    use util::definition::{Definition, DefinitionFilter, Model, Stage};
    use util::{batch, target, DefinitionMapping, TargetedMeta};
    use value::Value;

    //#[tokio::test]
    //#[traced_test]
    async fn test_insert() {
        let mut neo = EngineKind::neo4j_with_port(7688);

        neo.start().await.unwrap();

        let definition = Definition::new(
            "test",
            DefinitionFilter::AllMatch,
            DefinitionMapping::doc_to_graph(),
            Model::Document,
            "users".to_string(),
        )
        .await;
        neo.init_entity(&definition).await;

        neo.store(
            Stage::Plain,
            String::from("users"),
            &batch![target!(Value::text("test"), TargetedMeta::default())],
        )
        .await
        .unwrap();

        neo.store(
            Stage::Plain,
            String::from("users"),
            &batch![target!(Value::text("test"), TargetedMeta::default())],
        )
        .await
        .unwrap();

        neo.store(
            Stage::Mapped,
            String::from("users"),
            &batch![
                target!(
                    Value::node(
                        Value::int(0).as_int().unwrap(),
                        vec![Value::text("test").as_text().unwrap()],
                        BTreeMap::new(),
                    ),
                    TargetedMeta::default()
                )
            ],
        )
        .await
        .unwrap();

        neo.stop().await.unwrap();
    }

    //#[tokio::test]
    //#[traced_test]
    async fn test_insert_node() {
        let mut neo = EngineKind::neo4j();

        neo.start().await.unwrap();

        let definition = Definition::new(
            "test",
            DefinitionFilter::AllMatch,
            DefinitionMapping::doc_to_graph(),
            Model::Document,
            "users".to_string(),
        )
        .await;
        neo.init_entity(&definition).await;

        match neo.graph {
            None => {}
            Some(ref g) => {
                let query = query("CREATE (n:$($labels) {}) SET n = $props");
                let query = query
                    .param("props", HashMap::<String, String>::new())
                    .param("labels", vec![String::from("test"), String::from("test2")]);
                g.run(query).await.unwrap();

                let query = neo4rs::query("CREATE (n:$($labels) $props) SET n._id = $id");
                let node = Value::node(
                    Value::int(0).as_int().unwrap(),
                    vec![Value::text("test3").as_text().unwrap()],
                    BTreeMap::new(),
                );
                let node = node.as_node().unwrap();
                let mut props = HashMap::new();
                props.insert(
                    BoltString::new("key"),
                    BoltType::String(BoltString::new("val")),
                );
                props.insert(
                    BoltString::new("id"),
                    BoltType::Integer(BoltInteger::new(2)),
                );
                let query = query
                    .param("labels", node.labels.clone())
                    .param("props", BoltType::Map(BoltMap { value: props }))
                    .param("id", node.id);

                g.run(query).await.unwrap();

                let query = neo4rs::query(
                    "UNWIND $values AS row CREATE (n:addLabel:$(row.labels)) SET n = row.props SET n._id = row.id",
                );
                let node = Value::node(
                    Value::int(0).as_int().unwrap(),
                    vec![Value::text("test3").as_text().unwrap()],
                    Value::dict_from_pairs(vec![
                        ("key", Value::int(3)),
                        ("key2", Value::text("value2")),
                    ])
                    .as_dict()
                    .unwrap()
                    .values,
                );

                let query = query.param("values", vec![node]);

                g.run(query).await.unwrap();
            }
        }
        neo.stop().await.unwrap();
    }
}
