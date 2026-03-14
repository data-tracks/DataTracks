use crate::management::catalog::Catalog;
use engine::engine::Engine;
use flume::unbounded;
use processing::Scope;
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{error, info};
use util::definition::Stage;
use util::{target, Batch, Event, Runtimes, TargetedRecord};

pub struct Processor {
    catalog: Catalog,
}

const DEFINITIONS_THREADS: u32 = 5;

impl Processor {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    pub async fn start(
        self,
        _rt: Runtimes,
        outgoing: tokio::sync::broadcast::Sender<Batch<TargetedRecord>>,
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
            let outgoing = outgoing.clone();

            // Spawn a dedicated OS thread for this specific engine
            thread::spawn(move || {
                let rt = Builder::new_multi_thread()
                    .worker_threads(3)
                    .thread_name("processor") // No work-stealing possible
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    for _ in 0..DEFINITIONS_THREADS {
                        let definition = definition.clone();
                        let engines = engines.clone();
                        let mut engine = engines.into_iter().next().unwrap();
                        let startup_tx = startup_tx.clone();
                        let outgoing = outgoing.clone();

                        let id = id_counter;
                        id_counter += 1;
                        tokio::spawn(async move {
                            let mut join_set = JoinSet::new();

                            let processing = definition.processing();

                            let (next, rx) = match definition.algebra.scope() {
                                Scope::Tuple => {
                                    (async |engine: &mut Engine , partition_id, processed_data, records: Batch<TargetedRecord>| {
                                        match engine.store(partition_id, Stage::Process, definition.id, &processed_data).await {
                                            Ok(_) => {
                                                let ids: Vec<u64> = records.records.iter().map(|r| r.meta.id).collect();
                                                let _ = engine.statistic_sender.send(Event::Insert {
                                                    id: definition.id,
                                                    source: engine.id,
                                                    stage: Stage::Process,
                                                    ids,
                                                    first: Instant::now()
                                                });

                                                // Send original records to next phase
                                                let _ = outgoing.send(records);

                                                tokio::task::yield_now().await;
                                            }
                                            Err(err) => error!("Processing Store Error: {:?}", err),
                                        }
                                    }, definition.process_single.1.clone())
                                }
                                Scope::Multi | Scope::Join => {
                                    todo!("not yet added case for processing")
                                }
                            };


                            engine.start(&mut join_set).await.unwrap();
                            startup_tx.send(true).unwrap();

                            let name = format!("Processor {} {}", engine.engine_kind, id);

                            let mut hb_ticker = tokio::time::interval(Duration::from_secs(5));
                            let hb_name = name.clone();
                            let id = id.into();

                            loop {
                                tokio::select! {
                                    _ = hb_ticker.tick() => {
                                        let _ = engine.statistic_sender.send(Event::Heartbeat(hb_name.clone()));
                                    }

                                    res = rx.recv_async() => {
                                        let mut records = match res {
                                            Ok(r) => r,
                                            Err(_) => break, // Channel closed
                                        };

                                        // Efficiently drain pending records without over-allocating
                                        while let Ok(more) = rx.try_recv() {
                                            records.records.extend(more);
                                            if records.len() >= 100_000 { break; }
                                        }

                                        let length = records.len() as u64;

                                        let processed_data: Batch<_> = records.iter().flat_map(|r| {
                                            if let Some(v) = processing(&r.value) {
                                                Some(target!(v, r.meta.clone()))
                                            }else {
                                                None
                                            }
                                        }).collect();

                                        let partition_id = definition.partition_info.next(&id, &length).into();

                                        next(&mut engine, partition_id, processed_data, records).await;
                                    }
                                }
                            }
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
