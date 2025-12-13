use std::error::Error;
use std::sync::{Arc, Mutex};
use value::Value;

pub struct RecordQueue {
    values: Arc<Mutex<Vec<(Meta, Value)>>>,
}

impl RecordQueue {
    pub fn new() -> Self {
        RecordQueue {
            values: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn push<V: Into<Value>>(&self, meta: Meta, value: V) -> Result<(), Box<dyn Error + '_>> {
        self.values.lock()?.push((meta, value.into()));
        Ok(())
    }

    pub fn pop(&self) -> Option<(Meta, Value)> {
        let mut values = self.values.lock().ok()?;
        values.pop()
    }
}

impl Clone for RecordQueue {
    fn clone(&self) -> Self {
        RecordQueue {
            values: self.values.clone(),
        }
    }
}

pub struct Meta {
    pub name: Option<String>,
}
