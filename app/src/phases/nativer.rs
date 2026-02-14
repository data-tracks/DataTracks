use std::time::Duration;
use crate::management::catalog::Catalog;
use engine::engine::Engine;
use tokio::sync::broadcast::Sender;
use tokio::task::JoinSet;
use tracing::{debug, error};
use util::definition::Stage;
use util::{target, Batch, Event, TargetedRecord};

pub struct Nativer {
    catalog: Catalog,
}

impl Nativer {
    pub(crate) async fn start(
        &self,
        join_set: &mut JoinSet<()>,
        output: Sender<Batch<TargetedRecord>>,
    ) {
        //let catalog = self.catalog.clone();
        for definition in self.catalog.definitions().await {
            let engines = self
                .catalog
                .engines()
                .await
                .into_iter()
                .filter(|e| e.model() == definition.model)
                .collect::<Vec<Engine>>();

            for i in 0..5 {
                let definition = definition.clone();
                let engines = engines.clone();
                let output = output.clone();

                join_set.spawn(async move {
                    let rx = definition.native.1;

                    let entity = definition.entity;
                    let mut engine = engines.into_iter().next().unwrap();

                    let mapper = definition.mapping.build();

                    let name = format!("Nativer {} {}", engine.engine_kind, i);

                    let mut hb_ticker = tokio::time::interval(Duration::from_secs(5));
                    let hb_name = name.clone();

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

                                let length = records.len();
                                let ids: Vec<u64> = records.iter().map(|r| r.meta.id).collect();
                                let entity_name = entity.mapped.clone();

                                let mapped_data: Batch<_> = records.iter().map(|r| {
                                    target!(mapper(r.value.clone()), r.meta.clone())
                                }).collect();

                                match engine.store(Stage::Mapped, entity_name, &mapped_data).await {
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
    }

    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }
}
