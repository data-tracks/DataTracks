use crate::management::catalog::Catalog;
use anyhow::{anyhow, bail};
use async_trait::async_trait;
use engine::engine::Engine;
use flume::{Receiver, unbounded};
use processing::{Program, Scope};
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{error, info};
use util::definition::{Definition, Model, Stage};
use util::{Batch, Event, Runtimes, TargetedRecord, target};

pub struct Processor {
    catalog: Catalog,
}

const DEFINITIONS_THREADS: u32 = 5;

#[async_trait]
trait RecordProcessor: Send + Sync {
    async fn process(
        &mut self,
        id: u64,
        worker_id: u64,
        engine: Engine,
        definition: Definition,
    ) -> anyhow::Result<()>;
}

#[async_trait]
impl RecordProcessor for ProcessorType {
    async fn process(
        &mut self,
        id: u64,
        worker_id: u64,
        engine: Engine,
        definition: Definition,
    ) -> anyhow::Result<()> {
        match self {
            ProcessorType::Tuple(t) => t.process(id, worker_id, engine, definition).await,
        }
    }
}

enum ProcessorType {
    Tuple(TupleProcessor),
}

impl Processor {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    pub async fn start(
        self,
        _rt: Runtimes,
        _outgoing: tokio::sync::broadcast::Sender<Batch<TargetedRecord>>,
    ) -> anyhow::Result<()> {
        let definitions = self.catalog.definitions().await;

        let engines = self.catalog.engines().await;
        let mut id_counter = 0;
        let (startup_tx, startup_rx) = unbounded();
        let total_workers = DEFINITIONS_THREADS * definitions.len() as u32;

        for definition in definitions {
            let startup_tx = startup_tx.clone();
            let engines = engines
                .clone()
                .into_iter()
                .filter(|e| e.model() == definition.model)
                .collect::<Vec<Engine>>();
            //let outgoing = outgoing.clone();

            if !matches!(definition.model, Model::Relational) {
                continue;
            }

            // Spawn a dedicated OS thread for this specific engine
            thread::spawn(move || {
                let rt = Builder::new_multi_thread()
                    .worker_threads(3)
                    .thread_name("processor")
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    for i in 0..DEFINITIONS_THREADS {
                        let mut definition = definition.clone();
                        let engines = engines.clone();
                        let mut engine = engines.into_iter().next().unwrap();
                        let startup_tx = startup_tx.clone();

                        let id = id_counter;
                        id_counter += 1;
                        tokio::spawn(async move {
                            let mut join_set = JoinSet::new();

                            let mut strategy: ProcessorType = match definition.algebra.scope() {
                                Scope::Tuple => ProcessorType::Tuple(TupleProcessor {
                                    processing_engine: definition.processing(),
                                    rx: definition.process_single.1.clone(),
                                }),
                                Scope::Multi => todo!("MultiProcessor implementation"),
                                Scope::Join => todo!("JoinProcessor implementation"),
                            };

                            engine.start(&mut join_set).await.unwrap();
                            match startup_tx.send(true) {
                                Ok(_) => {}
                                Err(err) => error!("{}", err),
                            }

                            strategy
                                .process(i as u64, id, engine, definition)
                                .await
                                .unwrap();
                        });
                    }
                    std::future::pending::<()>().await;
                });
            });
        }

        for _ in 0..total_workers {
            startup_rx.recv()?;
        }
        info!("All processors started...");
        Ok(())
    }
}

struct TupleProcessor {
    processing_engine: Program,
    rx: Receiver<Batch<TargetedRecord>>,
}

#[async_trait]
impl RecordProcessor for TupleProcessor {
    async fn process(
        &mut self,
        id: u64,
        worker_id: u64,
        mut engine: Engine,
        definition: Definition,
    ) -> anyhow::Result<()> {
        let name = format!("Processor {} {}", engine.engine_kind, worker_id);

        let mut hb_ticker = tokio::time::interval(Duration::from_secs(5));
        let hb_name = name.clone();
        let id = id.into();

        let definition_id = definition.id;
        loop {
            let mut processing_engine = self.processing_engine.clone();
            tokio::select! {
                _ = hb_ticker.tick() => {
                    let _ = engine.statistic_sender.send(Event::Heartbeat(hb_name.clone()));
                }

                res = self.rx.recv_async() => {
                    let mut records = match res {
                        Ok(r) => r,
                        Err(_) => bail!("Could not receive"), // Channel closed
                    };

                    // Efficiently drain pending records without over-allocating
                    while let Ok(more) = self.rx.try_recv() {
                        records.records.extend(more);
                        if records.len() >= 100_000 { break; }
                    }

                    processing_engine.reset();

                     // Set the resource
                    processing_engine
                        .set_resource(
                            "$$source",
                            records.records.clone().into_iter().map(|d| d.value),
                        )?;
                    let meta = records.last().unwrap().meta.clone();

                    let processed_data = processing_engine.collect::<Vec<_>>();

                    let processed_data: Batch<_> = processed_data.into_iter()
                        .map(|d| {
                            target!(
                                d,
                                meta.clone()
                            )
                        })
                        .collect();
                    let partition_id = definition.partition_info.next(&id, &(records.len() as u64)).into();
                    //info!("{:?}", processed_data.records[0]);

                    // Store and notify
                    engine
                        .store(partition_id, Stage::Process, definition_id, &processed_data)
                        .await
                        .map_err(|e| anyhow!(e))?;

                    let ids: Vec<u64> = records.records.iter().map(|r| r.meta.id).collect();
                    let _ = engine.statistic_sender.send(Event::Insert {
                        id: definition_id,
                        source: engine.id,
                        stage: Stage::Process,
                        ids,
                        first: Instant::now(),
                    });

                    // Send original records to next phase
                    //let _ = outgoing.send(records);

                    tokio::task::yield_now().await;
                }
            }
        }
    }
}
