use crate::management::catalog::Catalog;
use engine::Engine;
use std::error::Error;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::info;
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
        let engine = self.select_engines(&value)?;

        let func = self.function()?;

        let value = func(value);

        info!("store {} - {}", engine, value);
        engine.store(value).await?;

        Ok(())
    }

    pub(crate) async fn start(self, joins: &mut JoinSet<()>) {
        joins.spawn(async move {
            loop {
                match self.queue.pop() {
                    Some((meta, value)) => self.next(meta, value).await.unwrap(),
                    None => sleep(Duration::from_millis(1)).await,
                }
            }
        });
    }

    pub(crate) fn add_engine(&mut self, id: usize, engine: Engine) {
        self.engines.push(engine);
    }

    fn select_engines(&self, value: &Value) -> Result<&Engine, Box<dyn Error + Send + Sync>> {
        Ok(self
            .engines
            .iter()
            .map(|e| (e.cost(value), e))
            .min_by(|(a), (b)| a.0.total_cmp(&b.0))
            .unwrap()
            .1)
    }

    fn function(&self) -> Result<Box<dyn Fn(Value) -> Value + Send>, Box<dyn Error + Send + Sync>> {
        Ok(Box::new(|value| value))
    }
}
