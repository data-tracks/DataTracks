use parking_lot::Mutex;
use redb::{Database, TableDefinition};
use speedy::{Readable, Writable};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tempfile::NamedTempFile;
use thiserror::Error;
use tracing::error;
use value::Value;

/// Error type for storage operations
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("File operation error: {0}")]
    FileError(String),
    #[error("Key not found")]
    KeyNotFound,
}

/// Sharable and thread-save value store
#[derive(Clone)]
pub struct ValueStore {
    inner: Arc<Mutex<SharedState>>,
    index: usize,
}

impl ValueStore {
    pub fn new() -> Self {
        Self::new_with_values(vec![])
    }

    pub fn new_with_values(values: Vec<Value>) -> Self {
        let store = ValueStore {
            inner: Arc::new(Mutex::new(SharedState::new(
                Storage::new_temp("temp").unwrap(),
            ))),
            index: 0,
        };
        store.append(values);
        store
    }

    pub fn add(&self, value: Value) -> Result<(), StorageError> {
        let mut inner = self.inner.lock();
        inner.counter += 1;
        let counter = inner.counter.into();
        inner.storage.write_value(counter, value);
        Ok(())
    }

    pub fn set_source(&mut self, source: usize) {
        self.index = source;
    }

    pub(crate) fn append(&self, values: Vec<Value>) {
        let mut inner = self.inner.lock();

        values.into_iter().for_each(|v| {
            inner.counter += 1;
            let counter: Value = inner.counter.into();
            inner.storage.write_value(counter.into(), v)
        });
    }

    pub fn drain(&self) -> Vec<Value> {
        let mut inner = self.inner.lock();
        inner.drain()
    }
}

pub struct SharedState {
    storage: CachedStorage,
    counter: usize,
}

impl SharedState {
    pub fn new(storage: Storage) -> Self {
        SharedState {
            storage: CachedStorage::new(storage, 10_000),
            counter: 0,
        }
    }

    pub(crate) fn clear(&mut self) {
        for i in 0..self.counter {
            self.storage.delete(i.into()).unwrap()
        }
        self.counter = 0;
    }

    pub(crate) fn drain(&mut self) -> Vec<Value> {
        self.storage.cache.drain().map(|(_, v)| v).collect()
    }
}

struct CachedStorage {
    //storage: Storage,
    cache: HashMap<Value, Value>,
    //counter: usize,
    //max: usize,
}

impl CachedStorage {
    pub(crate) fn read_value(&self, key: Value) -> Result<Value, StorageError> {
        self.cache
            .get(&key)
            .cloned()
            .ok_or(StorageError::KeyNotFound)
    }
    pub(crate) fn delete(&mut self, key: Value) -> Result<(), StorageError> {
        self.cache.remove(&key);

        Ok(())
    }
    pub(crate) fn write_value(&mut self, key: Value, value: Value) {
        self.cache.insert(key, value);
    }
}

impl CachedStorage {
    pub fn new(_storage: Storage, _cache: usize) -> Self {
        CachedStorage {
            cache: Default::default(),
        }
    }
}

pub struct Storage {
    path: Option<String>,
    table_name: String,
    database: Database,
}

impl Storage {
    /// Create a new storage instance with a temporary file.
    pub fn new_temp<S: AsRef<str>>(table_name: S) -> Result<Storage, StorageError> {
        let file = NamedTempFile::new().map_err(|e| StorageError::FileError(e.to_string()))?;
        let db = Database::create(file).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        Ok(Storage {
            path: None,
            table_name: table_name.as_ref().to_string(),
            database: db,
        })
    }

    /// Create a new storage instance from a specified file path.
    pub fn new_from_path<S: AsRef<str>>(file: S, table_name: S) -> Result<Storage, StorageError> {
        let db = Database::create(file.as_ref())
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let storage = Storage {
            path: Some(file.as_ref().to_string()),
            table_name: table_name.as_ref().to_string(),
            database: db,
        };
        Ok(storage)
    }

    /// Write a key-value pair to the storage.
    pub fn write(&self, key: Value, value: Vec<u8>) -> Result<(), StorageError> {
        let write_txn = self
            .database
            .begin_write()
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        {
            let mut table = write_txn
                .open_table(self.table())
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
            table
                .insert(key, value)
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        }
        write_txn
            .commit()
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    pub fn write_value(&mut self, key: Value, value: Value) -> Result<(), StorageError> {
        self.write(key, value.write_to_vec().unwrap())
    }

    pub fn read_value(&self, key: Value) -> Result<Value, StorageError> {
        Value::read_from_buffer(&self.read_u8(key)?)
            .map_err(|e| StorageError::DatabaseError(e.to_string()))
    }

    /// Read a value by its key from the storage.
    pub fn read_u8(&self, key: Value) -> Result<Vec<u8>, StorageError> {
        let read_txn = self
            .database
            .begin_read()
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        let table = read_txn
            .open_table(self.table())
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let value = table.get(key);
        let value = value
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?
            .map(|entry| entry.value().clone());
        value.ok_or(StorageError::KeyNotFound)
    }

    /// Delete a key-value pair from the storage.
    pub fn delete(&self, key: Value) -> Result<(), StorageError> {
        let delete_txn = self
            .database
            .begin_write()
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        {
            let mut table = delete_txn
                .open_table(self.table())
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
            table
                .remove(key)
                .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        }
        delete_txn
            .commit()
            .map_err(|e| StorageError::DatabaseError(e.to_string()))
    }

    fn table(&self) -> TableDefinition<Value, Vec<u8>> {
        TableDefinition::new(&self.table_name)
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            if let Err(e) = fs::remove_file(path) {
                eprintln!("Failed to remove file: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use value::Value;

    #[test]
    fn test_write_read() {
        let storage = Storage::new_temp("table".to_string()).unwrap();
        storage
            .write("test".into(), Value::text("David").write_to_vec().unwrap())
            .unwrap();
        assert_eq!(
            Value::read_from_buffer(&storage.read_u8("test".into()).unwrap()).unwrap(),
            Value::text("David")
        );
        assert!(storage.read_u8("nonexistent".into()).is_err());
    }

    #[test]
    fn test_write_permanently() {
        let path = "db_test";
        {
            let storage = Storage::new_from_path(path.to_string(), "table".to_string()).unwrap();
            storage
                .write("test".into(), Value::text("David").write_to_vec().unwrap())
                .unwrap();
        }
        assert!(!fs::exists(path).unwrap());
    }
}
