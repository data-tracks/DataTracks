use crate::management::catalog::Catalog;
use engine::Engine;
use std::error::Error;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{debug, info};
use util::definition::Definition;
use util::queue::{Meta, RecordQueue};
use value::Value;

pub struct Persister {
    engines: Vec<Engine>,
    pub queue: RecordQueue,
    catalog: Catalog,
}

impl Persister {
    pub fn new(catalog: Catalog) -> Persister {
        Persister {
            engines: vec![],
            queue: RecordQueue::new(),
            catalog,
        }
    }

    pub async fn next(&self, meta: Meta, value: Value) -> Result<(), Box<dyn Error + Send + Sync>> {
        let engine = self.select_engines(&value, &meta).await?;

        debug!("store {} - {}", engine, value);
        engine.next(meta, value).await?;

        Ok(())
    }

    pub async fn start(mut self, joins: &mut JoinSet<()>) {
        joins.spawn(async move {
            let mut engines = self.catalog.engines().await;
            self.engines.append(&mut engines);
            loop {
                match self.queue.pop() {
                    Some((meta, value)) => self.next(meta, value).await.unwrap(),
                    None => sleep(Duration::from_millis(1)).await,
                }
            }
        });
    }

    async fn select_engines(
        &self,
        value: &Value,
        meta: &Meta,
    ) -> Result<&Engine, Box<dyn Error + Send + Sync>> {
        let definitions = self.catalog.definitions().await;

        let mut definition = Definition::empty();

        for mut d in definitions {
            if d.matches(value, &meta) {
                definition = d;
            }
        }

        Ok(self
            .engines
            .iter()
            .map(|e| (e.cost(value, &definition), e))
            .min_by(|(a), (b)| a.0.total_cmp(&b.0))
            .unwrap()
            .1)
    }
}
