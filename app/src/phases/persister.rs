use crate::management::catalog::Catalog;
use crossbeam::channel::{unbounded, Receiver, Sender};
use engine::engine::Engine;
use engine::EngineKind;
use futures::StreamExt;
use std::error::Error;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{debug, info};
use util::definition::Definition;
use util::queue::{Meta, RecordContext, RecordQueue};
use value::Value;

pub struct Persister {
    engines: Vec<Engine>,
    catalog: Catalog,
}

impl Persister {
    pub fn new(catalog: Catalog) -> Persister {
        Persister {
            engines: vec![],
            catalog,
        }
    }

    pub async fn next(
        &self,
        value: Value,
        context: RecordContext,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (engine, context) = self.select_engines(&value, context).await?;

        debug!("store {} - {}", engine, value);
        engine.tx.send((value, context))?;

        Ok(())
    }

    pub async fn start(mut self, joins: &mut JoinSet<()>, mut rx: UnboundedReceiver<(Value, RecordContext)>) {
        joins.spawn(async move {
            let mut engines = self.catalog.engines().await;

            self.engines.append(&mut engines);

            loop {
                match rx.recv().await {
                    None => {}
                    Some((value, context)) => {
                        self.next(value, context).await.unwrap()
                    }
                }
            }
        });
    }

    async fn select_engines(
        &self,
        value: &Value,
        context: RecordContext,
    ) -> Result<(&Engine, RecordContext), Box<dyn Error + Send + Sync>> {
        let definitions = self.catalog.definitions().await;

        let mut definition = Definition::empty();

        for mut d in definitions {
            if d.matches(value, &context.meta) {
                definition = d;
            }
        }

        Ok((
            self.engines
                .iter()
                .map(|e| (e.cost(value, &definition), e))
                .min_by(|a, b| a.0.total_cmp(&b.0))
                .unwrap()
                .1,
            RecordContext {
                meta: context.meta,
                entity: Some(definition.entity),
            },
        ))
    }
}
