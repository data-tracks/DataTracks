use crate::management::catalog::Catalog;
use engine::engine::Engine;
use std::collections::HashMap;
use std::error::Error;
use tokio::task::JoinSet;
use tracing::{debug, error, info};
use util::definition::Stage;

pub struct Nativer {
    catalog: Catalog,
}

impl Nativer {
    pub(crate) async fn start(&self, join_set: &mut JoinSet<()>) {
        //let catalog = self.catalog.clone();
        for definition in self.catalog.definitions().await {
            let mut engines = self
                .catalog
                .engines()
                .await
                .into_iter()
                .filter(|e| e.model() == definition.model)
                .collect::<Vec<Engine>>();
            join_set.spawn(async move {
                let rx = definition.native.1;
                let entity = definition.entity;
                let mut engine = engines.into_iter().next().unwrap();

                let mapper = definition.mapping.build();
                loop {
                    if let Ok(record) = rx.recv_async().await {
                        match engine
                            .store(
                                Stage::Mapped,
                                entity.mapped.clone(),
                                record
                                    .values
                                    .into_iter()
                                    .map(|(v, m)| (mapper(v), m))
                                    .collect(),
                            )
                            .await
                        {
                            Ok(_) => {
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

    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }
}
