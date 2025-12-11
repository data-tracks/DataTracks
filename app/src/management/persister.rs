use crate::management::buffer::Buffer;
use engine::Engine;
use std::error::Error;
use value::Value;

pub struct Persister {
    engines: Vec<Engine>,
    buffer: Buffer,
}

impl Persister {
    pub fn new() -> Persister {
        Persister {
            engines: vec![],
            buffer: Buffer::new(),
        }
    }

    pub async fn next(&self, value: Value) -> Result<(), Box<dyn Error + Send + Sync>> {
        let engine = self.select_engines(&value)?;

        let func = self.function()?;

        let value = func(value);

        engine.store(value).await;

        Ok(())
    }

    fn select_engines(&self, value: &Value) -> Result<Engine, Box<dyn Error + Send + Sync>> {
        self.engines
            .iter()
            .map(|e| (e.cost(value), e))
            .min_by_key(|(k, v)| k)
    }

    fn function(&self) -> Result<Box<dyn Fn(Value) -> Value>, Box<dyn Error + Send + Sync>> {
        todo!()
    }
}
