use std::collections::BTreeMap;
use std::sync::{Arc};
use parking_lot::Mutex;
use value::Value;
/// Sharable and thread-save value store
#[derive(Clone)]
pub struct ValueReservoir {
    inner: Arc<Mutex<SharedState>>,
    pub index: usize,
}

impl Default for ValueReservoir {
    fn default() -> Self {
        Self::new()
    }
}

impl ValueReservoir {
    pub fn new() -> Self {
        Self::new_with_values(vec![], 0)
    }

    pub fn new_with_id(id: usize) -> Self {
        Self::new_with_values(vec![], id)
    }

    pub fn new_with_values(values: Vec<Value>, index: usize) -> Self {
        let store = ValueReservoir {
            inner: Arc::new(Mutex::new(SharedState::new())),
            index,
        };
        store.append(values);
        store
    }

    pub fn set_source(&mut self, source: usize) {
        self.index = source;
    }

    pub fn append(&self, values: Vec<Value>) {
        let mut inner = self.inner.lock();

        values.into_iter().for_each(|v| {
            inner.counter += 1;
            let counter: Value = inner.counter.into();
            inner.storage.write_value(counter, v)
        });
    }

    pub fn drain(&self) -> Vec<Value> {
        let mut inner = self.inner.lock();
        inner.drain()
    }
}

pub struct SharedState {
    storage: MemCache,
    counter: usize,
}

impl Default for SharedState {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedState {
    pub fn new() -> Self {
        SharedState {
            storage: MemCache::new(),
            counter: 0,
        }
    }

    pub fn drain(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.storage.cache)
            .into_values()
            .collect()
    }
}

struct MemCache {
    cache: BTreeMap<Value, Value>,
}

impl MemCache {
    pub fn delete(&mut self, key: Value) -> Result<(), String> {
        self.cache.remove(&key);

        Ok(())
    }
    pub fn write_value(&mut self, key: Value, value: Value) {
        self.cache.insert(key, value);
    }
}

impl MemCache {
    pub fn new() -> Self {
        MemCache {
            cache: Default::default(),
        }
    }
}
