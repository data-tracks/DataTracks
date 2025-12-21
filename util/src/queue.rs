use std::error::Error;
use std::sync::{Arc};
use chrono::Utc;
use crossbeam::queue::SegQueue;
use speedy::{Readable, Writable};
use tracing::error;
use value::Value;

pub struct RecordQueue {
    last_len: usize,
    alerting: bool,
    name: String,
    values: Arc<SegQueue<(Value, RecordContext)>>,
}

impl RecordQueue {
    pub fn new(name: String) -> Self {
        RecordQueue {
            last_len: 0,
            alerting: true,
            name,
            values: Arc::new(SegQueue::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub async fn push<V: Into<Value>>(
        &self,
        value: V,
        context: RecordContext,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {

        self.values.push((value.into(), context));
        Ok(())

    }

    pub fn pop(&mut self) -> Option<(Value, RecordContext)> {
        let len = self.values.len();
        if self.alerting && (len.saturating_sub(self.last_len) > 1_000 || self.last_len > 100_000) {
            error!("{} queue growing {}", self.name, len);
        }
        self.last_len = len;
        self.values.pop()
    }

}

impl Clone for RecordQueue {
    fn clone(&self) -> Self {
        RecordQueue {
            last_len: self.last_len,
            alerting: self.alerting,
            name: self.name.clone(),
            values: self.values.clone(),
        }
    }
}

#[derive(Clone, Debug, Writable, Readable, PartialEq)]
pub struct Meta {
    pub name: Option<String>,
}

impl Meta {
    pub fn new(name: Option<String>) -> Self {
        Meta { name }
    }
}

#[derive(Clone, Debug, Writable, Readable)]
pub struct RecordContext {
    pub meta: Meta,
    pub entity: Option<String>,
}

impl RecordContext {
    pub fn new(meta: Meta, entity: String) -> Self {
        RecordContext {
            meta,
            entity: Some(entity),
        }
    }
}
