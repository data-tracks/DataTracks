use crate::processing::Train;
use value::Value;
use redb::{Database, TableDefinition, TypeName};
use std::fs;
use speedy::{Readable, Writable};
use tempfile::NamedTempFile;
use thiserror::Error;
use tracing::error;

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


pub struct Storage {
    path: Option<String>,
    table_name: String,
    database: Database,
}

impl Storage {
    /// Create a new storage instance with a temporary file.
    pub fn new_temp(table_name: String) -> Result<Storage, StorageError> {
        let file = NamedTempFile::new().map_err(|e| StorageError::FileError(e.to_string()))?;
        let db = Database::create(file).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        Ok(Storage {
            path: None,
            table_name,
            database: db,
        })
    }

    /// Create a new storage instance from a specified file path.
    pub fn new_from_path(file: String, table_name: String) -> Result<Storage, StorageError> {
        let db = Database::create(file.clone())
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let storage = Storage {
            path: Some(file),
            table_name: table_name.to_string(),
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



impl redb::Value for Train {
    type SelfType<'a>
        = Value
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Value::read_from_buffer(data).expect("Failed to deserialize Train")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.write_to_vec().expect("Failed to serialize Value")
    }

    fn type_name() -> TypeName {
        TypeName::new("train")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use value::Value;

    #[test]
    fn test_write_read() {
        let storage = Storage::new_temp("table".to_string()).unwrap();
        storage.write("test".into(), Value::text("David").write_to_vec().unwrap()).unwrap();
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
            storage.write("test".into(), Value::text("David").write_to_vec().unwrap()).unwrap();
        }
        assert!(!fs::exists(path).unwrap());
    }
}
