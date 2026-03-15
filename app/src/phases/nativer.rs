use std::thread;
use crate::management::catalog::Catalog;
use engine::engine::Engine;
use std::time::Duration;
use flume::{unbounded};
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{error, info};
use processing::Scope;
use util::definition::Stage;
use util::{Batch, Event, Runtimes, target, TargetedRecord};

pub struct Nativer {
    catalog: Catalog,
}

const DEFINITION_THREADS: u32 = 3;

impl Nativer {
    pub(crate) async fn start(&self, _rt: Runtimes) -> anyhow::Result<()> {
        let definitions = self.catalog.definitions().await;

        let engines = self.catalog.engines().await;
        let mut id_counter = 0;
        let (startup_tx, startup_rx) = unbounded();
        let total_workers = DEFINITION_THREADS * definitions.len() as u32;

        //let catalog = self.catalog.clone();
        for definition in definitions {

            let startup_tx = startup_tx.clone();
            let engines = engines.clone()
                .into_iter()
                .filter(|e| e.model() == definition.model)
                .collect::<Vec<Engine>>();

            thread::spawn(move || {
                let rt = Builder::new_multi_thread()
                    .worker_threads(3)
                    .thread_name("nativer") // No work-stealing possible
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    for _ in 0..DEFINITION_THREADS {
                        let definition = definition.clone();
                        let engines = engines.clone();
                        let mut engine = engines.into_iter().next().unwrap();
                        let startup_tx = startup_tx.clone();


                        let id = id_counter;
                        id_counter += 1;
                        tokio::spawn(async move {
                            let mut join_set = JoinSet::new();

                            let tx:Box<dyn Fn(Batch<TargetedRecord>) -> () + Send> = match definition.algebra.scope() {
                                Scope::Tuple => {
                                    let tx = definition.process_single.0.clone();
                                    Box::new(move |records: Batch<TargetedRecord>| {
                                        tx.send(records).unwrap();
                                    })
                                },
                                Scope::Multi | Scope::Join => {
                                    let tx = definition.process_full.0;
                                    Box::new(move |records: Batch<TargetedRecord>| {
                                        tx.send(records.iter().map(|r| r.id()).collect()).unwrap();
                                    })
                                }
                            };

                            let rx = definition.native.1;
                            engine.start(&mut join_set).await.unwrap();
                            startup_tx.send(true).unwrap();

                            let mapper = definition.mapping.build();

                            let name = format!("Nativer {} {}", engine.engine_kind, id);

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
                                        let ids: Vec<u64> = records.iter().map(|r| r.meta.id).collect();

                                        let mapped_data: Batch<_> = records.iter().map(|r| {
                                            target!(mapper(r.value.clone()), r.meta.clone())
                                        }).collect();

                                        let partition_id = definition.partition_info.next(&id, &length).into();

                                        match engine.store(partition_id, Stage::Native, definition.id, &mapped_data).await {
                                            Ok(_) => {
                                                let _ = engine.statistic_sender.send(Event::Insert {
                                                    id: definition.id,
                                                    source: engine.id,
                                                    stage: Stage::Native,
                                                    ids,
                                                    first: Instant::now()
                                                });

                                                // Send original records to next phase
                                                tx(mapped_data);

                                                tokio::task::yield_now().await;
                                            }
                                            Err(err) => error!("Mapping Store Error: {:?}", err),
                                        }
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
        info!("All nativer started...");
        Ok(())
    }

    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }
}

