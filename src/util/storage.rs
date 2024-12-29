use crate::processing::Train;
use crate::value::Value;
use redb::{Database, Key, TableDefinition, TypeName};
use std::cmp::Ordering;
use std::fs;
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

pub struct Storage< Val>
where Val:redb::Value + 'static{
    path: Option<String>,
    table_name: String,
    table: TableDefinition<'static, Value, Val >,
    database: Database,
}


impl<Val: redb::Value> Storage<Val> {
    /// Create a new storage instance with a temporary file.
    pub fn new_temp(table_name: &str) -> Result<Storage<Val>, StorageError> {
        let file = NamedTempFile::new().map_err(|e| StorageError::FileError(e.to_string()))?;
        let db = Database::create(file).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        Ok(Storage {
            path: None,
            table_name: table_name.to_string(),
            table: TableDefinition::new(table_name.clone()),
            database: db,
        })
    }

    /// Create a new storage instance from a specified file path.
    pub fn new_from_path(file: String, table_name: &str) -> Result<Storage<Val>, StorageError> {
        let db = Database::create(file.clone()).map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let storage = Storage {
            path: Some(file),
            table_name: table_name.to_string(),
            table: TableDefinition::new(table_name.clone()),
            database: db,
        };
        Ok(storage)
    }


    /// Write a key-value pair to the storage.
    pub fn write(&self, key: Value, value: Val) -> Result<(), StorageError> {
        let write_txn = self.database.begin_write().map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        {
            let mut table = write_txn.open_table(self.table).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
            table.insert(key, value).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        }
        write_txn.commit().map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    /// Read a value by its key from the storage.
    pub fn read(&self, key: Value) -> Result<Val, StorageError> {
        let read_txn = self.database.begin_read().map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        let table = read_txn.open_table(self.table).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        table
            .get(key)
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?
            .map(|entry| Ok(entry.value()))
            .unwrap_or_else(|| Err(StorageError::KeyNotFound))
    }

    /// Delete a key-value pair from the storage.
    pub fn delete(&self, key: Value) -> Result<(), StorageError> {
        let delete_txn = self.database.begin_write().map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        {
            let mut table = delete_txn.open_table(self.table).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
            table.remove(key).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        }
        delete_txn.commit().map_err(|e| StorageError::DatabaseError(e.to_string()))
    }

}

impl<Val: redb::Value> Clone for Storage<Val> {
    fn clone(&self) -> Self {
        if let Some(path) = self.path.clone() {
            Storage::new_from_path(path.clone(), &self.table_name).unwrap()
        }else {
            Storage::new_temp(&self.table_name).unwrap()
        }
    }
}


impl<Val: redb::Value> Drop for Storage<Val> {
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

impl Key for Value {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let val1:Value = postcard::from_bytes(data1).expect("Failed to deserialize Value");
        let val2:Value = postcard::from_bytes(data2).expect("Failed to deserialize Value");
        val1.cmp(&val2)
    }
}

impl redb::Value for Train  {
    type SelfType<'a> = Value where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a
    {
        postcard::from_bytes(data).expect("Failed to deserialize Train")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b
    {
        postcard::to_allocvec(value).expect("Failed to serialize Value")
    }

    fn type_name() -> TypeName {
        TypeName::new("train")
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_write_read() {
        let storage:Storage<Value> = Storage::new_temp("table").unwrap();
        storage.write("test".into(), Value::text("David")).unwrap();
        assert_eq!(storage.read("test".into()).unwrap(), Value::text("David"));
        assert!(storage.read("nonexistent".into()).is_err());
    }

    #[test]
    fn test_write_permanently() {
        let path = "db_test";
        {
            let storage = Storage::new_from_path(path.to_string(), "table").unwrap();
            storage.write("test".into(), Value::text("David")).unwrap();
        }
        assert!(!fs::exists(path).unwrap());
    }
}
