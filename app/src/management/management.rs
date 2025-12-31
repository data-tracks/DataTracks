use crate::management::catalog::Catalog;
use crate::phases::Persister;
use crate::phases::mapper::Nativer;
use engine::EngineKind;
use flume::{Sender, unbounded};
use sink::dummy::DummySink;
use sink::kafka::Kafka;
use statistics::Event;
use std::error::Error;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{error, info};
use util::definition::{Definition, DefinitionFilter, Model};
use util::DefinitionMapping;
use value::Value;

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

        let mut joins: JoinSet<()> = JoinSet::new();

        let statistic_tx = statistics::start(&mut self.joins).await;
        let mut persister = Persister::new(self.catalog.clone()).await?;
        let nativer = Nativer::new(self.catalog.clone());

        self.init_engines(statistic_tx.clone()).await?;

        self.init_definitions(statistic_tx).await?;

        persister.start_distributor(&mut self.joins).await;

        let kafka = self.start_sinks(persister).await?;

        nativer.start(&mut self.joins).await;

        tokio::select! {
                _ = ctrl_c_signal => {
                    info!("#️⃣ Ctrl-C received!");
                }
                Some(res) = joins.join_next() => {
                    if let Err(e) = res {
                        error!("\nFatal Error: A core task crashed: {:?}", e);
                    }
                }
        }

        info!("Stopping kafka...");
        kafka.stop().await?;

        info!("Stopping engines...");
        self.catalog.stop().await?;

        // Clean up all remaining running tasks
        info!("🧹 Aborting remaining tasks...");
        joins.abort_all();
        while joins.join_next().await.is_some() {}

        info!("✅ All services shut down. Exiting.");

        Ok(())
    }

    async fn init_definitions(
        &mut self,
        statistic_tx: Sender<Event>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.catalog
            .add_definition(
                Definition::new(
                    DefinitionFilter::MetaName(String::from("doc")),
                    DefinitionMapping::document(),
                    Model::Document,
                    String::from("doc"),
                ),
                statistic_tx.clone(),
            )
            .await?;

        self.catalog
            .add_definition(
                Definition::new(
                    DefinitionFilter::MetaName(String::from("relational")),
                    DefinitionMapping::document(),
                    Model::Relational,
                    String::from("relational"),
                ),
                statistic_tx.clone(),
            )
            .await?;

        self.catalog
            .add_definition(
                Definition::new(
                    DefinitionFilter::MetaName(String::from("graph")),
                    DefinitionMapping::doc_to_graph(),
                    Model::Graph,
                    String::from("graph"),
                ),
                statistic_tx,
            )
            .await?;
        Ok(())
    }

    async fn init_engines(
        &mut self,
        statistic_tx: Sender<Event>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let engines = EngineKind::start_all(&mut self.joins, statistic_tx.clone()).await?;
        for engine in engines.into_iter() {
            self.catalog.add_engine(engine, statistic_tx.clone()).await;
        }

        Ok(())
    }

    async fn start_sinks(
        &mut self,
        persister: Persister,
    ) -> Result<Kafka, Box<dyn Error + Send + Sync>> {
        let (tx, rx) = unbounded();

        let kafka = sink::kafka::start(&mut self.joins, tx.clone()).await?;

        persister.start(&mut self.joins, rx).await;

        let amount = 20_000;

        let clone_graph = kafka.clone();
        self.joins.spawn(async move {
            loop {
                clone_graph.send_value_graph().await.unwrap();
                sleep(Duration::from_nanos(100)).await;
            }
        });

        for _ in 0..amount {
            let tx = tx.clone();
            self.joins.spawn(async {
                let mut dummy = DummySink::new(Value::text("test"), Duration::from_millis(100));
                dummy.start(String::from("relational"), tx).await;
            });
        }

        let clone_rel = kafka.clone();
        self.joins.spawn(async move {
            loop {
                clone_rel.send_value_relational().await.unwrap();
                sleep(Duration::from_nanos(10)).await;
            }
        });

        for _ in 0..amount {
            let tx = tx.clone();
            self.joins.spawn(async {
                let mut dummy = DummySink::new(Value::dict_from_pairs(vec![("test", Value::text("test")),("key2", Value::text("test2"))]), Duration::from_millis(1));
                dummy.start(String::from("doc"), tx).await;
            });
        }

        let clone_doc = kafka.clone();
        self.joins.spawn(async move {
            loop {
                clone_doc.send_value_doc().await.unwrap();
                sleep(Duration::from_millis(1)).await;
            }
        });

        for _ in 0..amount {
            let tx = tx.clone();
            self.joins.spawn(async {
                let mut dummy = DummySink::new(Value::dict_from_pairs(vec![("id", Value::text("test")),("label", Value::text("test2")),("properties", Value::dict_from_pairs(vec![]))]), Duration::from_millis(10));
                dummy.start(String::from("graph"), tx).await;
            });
        }

        Ok(kafka)
    }
}
