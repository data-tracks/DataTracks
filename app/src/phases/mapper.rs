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

                    let name = format!("Nativer {}", i);

                    loop {
                        engine
                            .statistic_sender
                            .send(Event::Heartbeat(name.clone()))
                            .unwrap();
                        if let Ok(records) = rx.recv_async().await {
                            let length = records.len();
                            match engine
                                .store(
                                    Stage::Mapped,
                                    entity.mapped.clone(),
                                    records
                                        .clone()
                                        .into_iter()
                                        .map(|TargetedRecord { value, meta }| {
                                            target!(mapper(value), meta)
                                        })
                                        .collect(),
                                )
                                .await
                            {
                                Ok(_) => {
                                    engine
                                        .statistic_sender
                                        .send(Event::Insert(
                                            definition.id,
                                            length,
                                            engine.id,
                                            Stage::Mapped,
                                        ))
                                        .unwrap();

                                    let _ = output.send(records);

                                    debug!("mapped")
                                }
                                Err(err) => {
                                    error!("{:?}", err)
                                }
                            };
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
