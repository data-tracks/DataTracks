use crate::management::catalog::Catalog;
use engine::engine::Engine;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::broadcast::Sender;
use tracing::error;
use util::definition::Stage;
use util::{Batch, Event, TargetedRecord, target, Runtimes};

pub struct Nativer {
    catalog: Catalog,
}

impl Nativer {
    pub(crate) async fn start(
        &self,
        rt: Runtimes,
        output: Sender<Batch<TargetedRecord>>,
    ) {
        let rt_mapper = Builder::new_multi_thread()
            .worker_threads(3)
            .thread_name("nativer")
            .enable_all()
            .build()
            .unwrap();


        let mut id_counter = 0;
        //let catalog = self.catalog.clone();
        for definition in self.catalog.definitions().await {
            let engines = self
                .catalog
                .engines()
                .await
                .into_iter()
                .filter(|e| e.model() == definition.model)
                .collect::<Vec<Engine>>();

            for _ in 0..5 {
                let definition = definition.clone();
                let engines = engines.clone();
                let output = output.clone();

                let id = id_counter;
                id_counter += 1;
                rt_mapper.spawn(async move {
                    let rx = definition.native.1;

                    let mut engine = engines.into_iter().next().unwrap();

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

                                match engine.store(partition_id, Stage::Mapped, definition.id, &mapped_data).await {
                                    Ok(_) => {
                                        let _ = engine.statistic_sender.send(Event::Insert {
                                            id: definition.id,
                                            size: length,
                                            source: engine.id,
                                            stage: Stage::Mapped,
                                            ids
                                        });

                                        // Send original records to next phase
                                        let _ = output.send(records);
                                    }
                                    Err(err) => error!("Mapping Store Error: {:?}", err),
                                }
                            }
                        }
                    }
                });
            }
        }
        rt.add_runtime(rt_mapper);
    }

    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }
}
