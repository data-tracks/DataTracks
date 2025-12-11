use std::error::Error;
use std::sync::{Arc, Mutex};
use value::Value;

pub struct Buffer {
    values: Arc<Mutex<Vec<Value>>>,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            values: Arc::new(Mutex::new(vec![])),
        }
    }

    fn push(&mut self, value: Value) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.values.lock()?.push(value);
        Ok(())
    }

    fn pull(&mut self) -> Option<Value> {
        self.values.lock().ok()?.pop()
    }
}
