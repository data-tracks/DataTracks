use crate::management::catalog::Catalog;
use crate::phases::Persister;
use crossbeam::channel::{unbounded, Receiver, RecvError};
use engine::EngineKind;
use futures::future::join_all;
use sink::dummy::DummySink;
use sink::kafka::Kafka;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use futures::channel;
use tokio::runtime::Handle;
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};
use tracing::{error, info};
use util::definition::{Definition, DefinitionFilter, Model};
use util::queue::{Meta, RecordContext};
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

        let persister = self.start_engines().await?;

        self.catalog
            .add_definition(Definition::new(
                DefinitionFilter::MetaName(String::from("doc")),
                Model::Document,
                String::from("doc"),
            ))
            .await?;

        self.catalog
            .add_definition(Definition::new(
                DefinitionFilter::MetaName(String::from("relational")),
                Model::Relational,
                String::from("relational"),
            ))
            .await?;

        self.catalog
            .add_definition(Definition::new(
                DefinitionFilter::MetaName(String::from("graph")),
                Model::Graph,
                String::from("graph"),
            ))
            .await?;

        self.start_distributor().await;

        let kafka = self.start_sinks(persister).await?;

        joins.spawn(async {
            let metrics = Handle::current().metrics();

            loop {
                info!("Active tasks: {}", metrics.num_alive_tasks());
                info!("Worker threads: {}", metrics.num_workers());
                info!("Blocking threads: {}", metrics.num_blocking_threads());
                info!(
                    "Budget forced yields: {}",
                    metrics.budget_forced_yield_count()
                );
                sleep(Duration::from_secs(5)).await;
            }
        });

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

    async fn start_engines(&mut self) -> Result<Persister, Box<dyn Error + Send + Sync>> {
        let persister = Persister::new(self.catalog.clone());

        let engines = EngineKind::start_all(&mut self.joins).await?;
        for engine in engines.into_iter() {
            self.catalog.add_engine(engine).await;
        }

        Ok(persister)
    }

    async fn start_distributor(&mut self) {
        for mut engine in self.catalog.engines().await {
            let clone = engine.clone();

            self.joins.spawn(async move {
                let mut error_count = 0;
                let mut count = 0;
                let mut first_ts = Instant::now();
                let mut buckets: HashMap<String, Vec<Value>> = HashMap::new();

                loop {
                    if first_ts.elapsed().as_millis() > 10 || count > 1_000_000 {
                        // try to drain the "buffer"

                        for (entity, values) in buckets.drain() {
                            match clone.store(entity.clone(), values.clone()).await {
                                Ok(_) => {
                                    error_count = 0;
                                    count = 0;
                                }
                                Err(err) => {
                                    error_count += 1;
                                    error!("Error during distribution to engines {}", err);
                                    let errors: Vec<_> = values
                                        .into_iter()
                                        .filter_map(|v| {
                                            let res = engine
                                                .tx
                                                .send((
                                                    v,
                                                    RecordContext::new(
                                                        Meta::new(Some(entity.clone())),
                                                        entity.clone(),
                                                    ),
                                                ))
                                                .map_err(|err| format!("{:?}", err));
                                            match res {
                                                Ok(_) => None,
                                                Err(err) => Some(err)
                                            }
                                        })
                                        .collect();

                                    if !errors.is_empty() {
                                        error!("{}", errors.first().unwrap())
                                    }

                                    if error_count > 1_000 {
                                        sleep(Duration::from_secs(1)).await;
                                    } else if error_count > 1_000_000 {
                                        panic!("Over 1 Mio retries")
                                    }
                                }
                            }
                        }
                    }


                    match engine.rx.try_recv() {
                        Err(_) => sleep(Duration::from_millis(1)).await, // max shift after max timeout for sending finished chunk out
                        Ok((v, context)) => {
                            let entity = context.entity.unwrap_or("_stream".to_string());

                            if buckets.is_empty() {
                                first_ts = Instant::now();
                            }
                            buckets.entry(entity).or_default().push(v);
                            count += 1;
                        }
                    }
                }
            });
        }
    }

    async fn start_sinks(
        &mut self,
        persister: Persister,
    ) -> Result<Kafka, Box<dyn Error + Send + Sync>> {
        //let (tx, rx) = unbounded::<(Value, RecordContext)>();

        let (tx, rx) = unbounded_channel();

        let kafka = sink::kafka::start(&mut self.joins, tx.clone()).await?;

        persister.start(&mut self.joins, rx).await;

        let amount = 2000;

        let clone_graph = kafka.clone();
        self.joins.spawn(async move {
            loop {
                clone_graph.send_value_graph().await.unwrap();
                sleep(Duration::from_millis(100)).await;
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
                sleep(Duration::from_millis(10)).await;
            }
        });

        for _ in 0..amount {
            let tx = tx.clone();
            self.joins.spawn(async {
                let mut dummy = DummySink::new(Value::text("test"), Duration::from_millis(1));
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
                let mut dummy = DummySink::new(Value::text("test"), Duration::from_millis(10));
                dummy.start(String::from("graph"), tx).await;
            });
        }

        Ok(kafka)
    }
}
