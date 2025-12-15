use std::error::Error;
use std::ops::Sub;
use std::sync::{Arc, Mutex};
use tracing::error;
use value::Value;

pub struct RecordQueue {
    last_len: usize,
    values: Arc<Mutex<Vec<(Meta, Value)>>>,
}

impl RecordQueue {
    pub fn new() -> Self {
        RecordQueue {
            last_len: 0,
            values: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn push<V: Into<Value>>(&self, meta: Meta, value: V) -> Result<(), Box<dyn Error + '_>> {
        self.values.lock()?.push((meta, value.into()));
        Ok(())
    }

    pub fn pop(&mut self) -> Option<(Meta, Value)> {
        let mut values = self.values.lock().ok()?;
        let len = values.len();
        if len.saturating_sub(self.last_len) > 10 {
            error!("queue growing {}", len);
        }
        self.last_len = len;
        values.pop()
    }
}

impl Clone for RecordQueue {
    fn clone(&self) -> Self {
        RecordQueue {
            last_len: self.last_len,
            values: self.values.clone(),
        }
    }
}

pub struct Meta {
    pub name: Option<String>,
}
