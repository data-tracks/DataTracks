use std::fs;
use crate::value::Value;
use redb::{Database, TableDefinition, TypeName};
use tempfile::NamedTempFile;
use thiserror::Error;

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

#[derive(Clone)]
pub struct Storage<'a> {
    path: Option<String>,
    table_name: String,
    table: TableDefinition<'a, String, Value>,
    database: Database,
}

impl<'a> Storage<'a> {
    /// Create a new storage instance with a temporary file.
    pub fn new_temp(table_name: &'a str) -> Result<Storage<'a>, StorageError> {
        let file = NamedTempFile::new().map_err(|e| StorageError::FileError(e.to_string()))?;
        let db = Database::create(file).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        Ok(Storage {
            path: None,
            table_name: table_name.to_string(),
            table: TableDefinition::new(table_name),
            database: db,
        })
    }

    /// Create a new storage instance from a specified file path.
    pub fn new_from_path(file: &str, table_name: &'a str) -> Result<Storage<'a>, StorageError> {
        let db = Database::create(file).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        Ok(Storage {
            path: Some(file.to_string()),
            table_name: table_name.to_string(),
            table: TableDefinition::new(table_name),
            database: db,
        })
    }

    /// Write a key-value pair to the storage.
    pub fn write(&self, key: &str, value: &Value) -> Result<(), StorageError> {
        let write_txn = self.database.begin_write().map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        {
            let mut table = write_txn.open_table(self.table).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
            table.insert(key.to_string(), value).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        }
        write_txn.commit().map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Read a value by its key from the storage.
    pub fn read(&self, key: &str) -> Result<Value, StorageError> {
        let read_txn = self.database.begin_read().map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        let table = read_txn.open_table(self.table).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        table
            .get(key.to_string())
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?
            .map(|entry| Ok(entry.value()))
            .unwrap_or_else(|| Err(StorageError::KeyNotFound))
    }

    /// Delete a key-value pair from the storage.
    pub fn delete(&self, key: &str) -> Result<(), StorageError> {
        let delete_txn = self.database.begin_write().map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        {
            let mut table = delete_txn.open_table(self.table).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
            table.remove(key.to_string()).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        }
        delete_txn.commit().map_err(|e| StorageError::DatabaseError(e.to_string()))
    }
}

impl Drop for Storage<'_> {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            if let Err(e) = fs::remove_file(path) {
                eprintln!("Failed to remove file: {}", e);
            }
        }
    }
}

impl redb::Value for Value {
    type SelfType<'a> = Value where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        postcard::from_bytes(data).expect("Failed to deserialize Value")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        postcard::to_allocvec(value).expect("Failed to serialize Value")
    }

    fn type_name() -> TypeName {
        TypeName::new("value")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_write_read() {
        let storage = Storage::new_temp("table").unwrap();
        storage.write("test", &Value::text("David")).unwrap();
        assert_eq!(storage.read("test").unwrap(), Value::text("David"));
        assert!(storage.read("nonexistent").is_err());
    }

    #[test]
    fn test_write_permanently() {
        let path = "db_test";
        {
            let storage = Storage::new_from_path(path, "table").unwrap();
            storage.write("test", &Value::text("David")).unwrap();
        }
        assert!(!fs::exists(path).unwrap());
    }
}
