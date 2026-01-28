use crate::management::catalog::Catalog;
use crate::phases::Persister;
use crate::phases::mapper::Nativer;
use engine::EngineKind;
use flume::{Sender, unbounded};
use sink::dummy::DummySink;
use sink::kafka::Kafka;
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tracing::{error, info};
use util::definition::{Definition, DefinitionFilter, Model};
use util::runtimes::Runtimes;
use util::{DefinitionMapping, Event, InitialMeta, RelationalType, log_channel};
use value::Value;

#[derive(Default)]
pub struct Manager {
    catalog: Catalog,
    joins: JoinSet<()>,
    runtimes: Runtimes,
}

impl Manager {
    pub fn new() -> Manager {
        Manager {
            joins: JoinSet::new(),
            catalog: Catalog::default(),
            runtimes: Runtimes::new(),
        }
    }

    pub fn start(mut self) -> anyhow::Result<()> {
        let ctrl_c_signal = tokio::signal::ctrl_c();

        let main_rt = Builder::new_multi_thread()
            .worker_threads(8)
            .thread_name("main-rt")
            .enable_all()
            .build()?;

        let trash_rt = Builder::new_multi_thread()
            .worker_threads(8)
            .thread_name("trash-rt")
            .enable_all()
            .build()?;

        self.runtimes.add_runtime(trash_rt);

        let (tx, rx) = unbounded::<Event>();

        let rt = self.runtimes.clone();

        let statistic_tx = statistics::start(rt, tx, rx);
        let mut persister = Persister::new(self.catalog.clone())?;
        let nativer = Nativer::new(self.catalog.clone());

        self.init_engines(statistic_tx.clone())?;

        let rt = self.runtimes.clone();

        main_rt.block_on(async move {
            let mut joins: JoinSet<()> = JoinSet::new();

            self.init_definitions(statistic_tx).await?;

            persister.start_distributor(&mut self.joins).await;

            let kafka = self.start_sinks(persister, rt).await?;

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
            anyhow::Ok::<()>(())
        })
    }

    async fn init_definitions(
        &mut self,
        statistic_tx: Sender<Event>,
    ) -> anyhow::Result<()> {
        self.catalog
            .add_definition(
                Definition::new(
                    "Document test",
                    DefinitionFilter::MetaName(String::from("doc")),
                    DefinitionMapping::document(),
                    Model::Document,
                    String::from("doc"),
                )
                .await,
                statistic_tx.clone(),
            )
            .await?;

        self.catalog
            .add_definition(
                Definition::new(
                    "Relational test",
                    DefinitionFilter::MetaName(String::from("relational")),
                    DefinitionMapping::tuple_to_relational(vec![
                        ("name".to_string(), RelationalType::Text),
                        ("age".to_string(), RelationalType::Integer),
                    ]),
                    Model::Relational,
                    String::from("relational"),
                )
                .await,
                statistic_tx.clone(),
            )
            .await?;

        self.catalog
            .add_definition(
                Definition::new(
                    "Graph test",
                    DefinitionFilter::MetaName(String::from("graph")),
                    DefinitionMapping::doc_to_graph(),
                    Model::Graph,
                    String::from("graph"),
                )
                .await,
                statistic_tx,
            )
            .await?;
        Ok(())
    }

    fn init_engines(
        &mut self,
        statistic_tx: Sender<Event>,
    ) -> anyhow::Result<()> {
        let mut catalog = self.catalog.clone();

        let (tx, rx) = unbounded();

        let engines = thread::spawn(move || {
            let rt = Builder::new_current_thread()
                .worker_threads(4)
                .thread_name("engine-rt")
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let mut joins: JoinSet<()> = JoinSet::new();

                let engines = EngineKind::start_all(&mut joins, statistic_tx.clone())
                    .await
                    .unwrap();
                for engine in engines.into_iter() {
                    catalog.add_engine(engine, statistic_tx.clone()).await;
                }
                tx.send_async(true).await.unwrap();

                joins.join_all().await;
            });
        });
        self.runtimes.add_handle(engines);

        rx.recv()?;

        Ok(())
    }

    async fn start_sinks(
        &mut self,
        persister: Persister,
        rt: Runtimes,
    ) -> anyhow::Result<Kafka> {
        let (tx, rx) = unbounded();
        log_channel(tx.clone(), "Sink Input").await;

        let kafka = sink::kafka::start(&mut self.joins, tx.clone()).await?;

        persister.start(rx, rt);

        self.build_dummy(tx, &kafka);

        Ok(kafka)
    }

    fn build_dummy(&mut self, tx: Sender<(Value, InitialMeta)>, _: &Kafka) {
        let amount = 2_0;

        /*let clone_graph = kafka.clone();
        self.joins.spawn(async move {
            loop {
                clone_graph.send_value_graph().await.unwrap();
                sleep(Duration::from_nanos(100)).await;
            }
        });*/

        for _ in 0..amount {
            let tx = tx.clone();
            self.joins.spawn(async {
                let mut dummy = DummySink::new(
                    Value::array(vec![Value::text("David"), Value::int(31)]),
                    Duration::from_millis(10),
                );
                dummy.start(String::from("relational"), tx).await;
            });
        }

        /*let clone_rel = kafka.clone();
        self.joins.spawn(async move {
            loop {
                clone_rel.send_value_relational().await.unwrap();
                sleep(Duration::from_secs(10)).await;
            }
        });*/

        for _ in 0..amount {
            let tx = tx.clone();
            self.joins.spawn(async {
                let mut dummy = DummySink::new(
                    Value::dict_from_pairs(vec![
                        ("test", Value::text("test")),
                        ("key2", Value::text("test2")),
                    ]),
                    Duration::from_millis(10),
                );
                dummy.start(String::from("doc"), tx).await;
            });
        }

        /*let clone_doc = kafka.clone();
        self.joins.spawn(async move {
            loop {
                clone_doc.send_value_doc().await.unwrap();
                sleep(Duration::from_secs(10)).await;
            }
        });*/

        for _ in 0..amount {
            let tx = tx.clone();
            self.joins.spawn(async {
                let mut dummy = DummySink::new(
                    Value::dict_from_pairs(vec![
                        ("id", Value::text("test")),
                        ("label", Value::text("test2")),
                        (
                            "properties",
                            Value::dict_from_pairs(vec![("test", Value::text("text"))]),
                        ),
                    ]),
                    Duration::from_millis(10),
                );
                dummy.start(String::from("graph"), tx).await;
            });
        }
    }
}
