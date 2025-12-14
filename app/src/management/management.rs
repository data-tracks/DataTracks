use crate::management::catalog::Catalog;
use util::definition::{Definition, DefinitionFilter, Model};
use engine::Engine;
use reqwest::blocking::Client;
use sink::kafka::Kafka;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{error, info};
use crate::phases::Persister;
use util::queue::RecordQueue;
use value::Time;

#[derive(Default)]
pub struct Manager {
    catalog: Catalog,
    joins: JoinSet<()>,
}

impl Manager {
    pub fn new() -> Manager {
        Manager {
            joins: JoinSet::new(),
            catalog: Catalog::default(),
        }
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let ctrl_c_signal = tokio::signal::ctrl_c();

        let mut join_set: JoinSet<()> = JoinSet::new();

        self.catalog
            .add_definition(Definition::new(
                DefinitionFilter::MetaName(String::from("doc")),
                Model::Document,
                String::from("doc"),
            ))
            .await;

        self.catalog
            .add_definition(Definition::new(
                DefinitionFilter::MetaName(String::from("relational")),
                Model::Relational,
                String::from("relational"),
            ))
            .await;

        self.catalog
            .add_definition(Definition::new(
                DefinitionFilter::MetaName(String::from("graph")),
                Model::Graph,
                String::from("graph"),
            ))
            .await;

        let kafka = self.start_engines().await?;

        tokio::select! {
                _ = ctrl_c_signal => {
                    info!("#️⃣ Ctrl-C received!");
                }
                Some(res) = join_set.join_next() => {
                    if let Err(e) = res {
                        error!("\nFatal Error: A core task crashed: {:?}", e);
                    }
                }
        }

        info!("Stopping kafka...");
        kafka.stop().await?;

        // Clean up all remaining running tasks
        info!("🧹 Aborting remaining tasks...");
        join_set.abort_all();
        while join_set.join_next().await.is_some() {}

        info!("✅  All services shut down. Exiting.");

        Ok(())
    }

    async fn start_engines(&mut self) -> Result<Kafka, Box<dyn Error + Send + Sync>> {
        let mut persister = Persister::new(self.catalog.clone());

        for (name, engine) in Engine::start_all().await?.into_iter().enumerate() {
            persister.add_engine(name, engine);
        }

        let kafka = sink::kafka::start(&mut self.joins, persister.queue.clone()).await?;
        persister.start(&mut self.joins).await;

        let clone_rel = kafka.clone();
        let clone_doc = kafka.clone();
        let clone_graph = kafka.clone();
        self.joins.spawn(async move {
            loop {
                clone_graph.send_value_graph().await.unwrap();
                sleep(Duration::from_millis(100)).await;
            }
        });

        self.joins.spawn(async move {
            loop {
                clone_rel.send_value_relational().await.unwrap();
                sleep(Duration::from_millis(10)).await;
            }
        });

        self.joins.spawn(async move {
            loop {
                clone_doc.send_value_doc().await.unwrap();
                sleep(Duration::from_millis(1)).await;
            }
        });

        Ok(kafka)
    }
}
