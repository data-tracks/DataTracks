use crate::management::catalog::Catalog;
use std::collections::HashMap;
use tokio::task::JoinSet;
use tracing::info;

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
                .map(|eng| (eng.id, eng))
                .collect::<HashMap<_, _>>();
            join_set.spawn(async move {
                let rx = definition.native.1;
                let entity = definition.entity;

                loop {
                    if let Ok(ctx) = rx.recv_async().await {
                        let engine_id = ctx.engine_id;
                        if let Some(engine) = engines.get_mut(&engine_id) {
                            let entity = entity.plain.clone();
                            if let Ok(v) = engine.read(entity, ctx.ids).await
                                && !v.is_empty()
                            {
                                info!("{:?}", v)
                            };
                        }
                    }
                }
            });
        }
    }

    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }
}
