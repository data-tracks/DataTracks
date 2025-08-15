use moka::sync::Cache;
use redb::{Database, Durability, ReadableDatabase, TableDefinition};
use speedy::{Readable, Writable};
use std::fs;
use tempfile::NamedTempFile;
use uuid::Uuid;
use value::Value;
use value::train::{Train, TrainId};

pub struct Storage {
    path: Option<String>,
    table_name: String,
    database: Database,
    cache: Cache<TrainId, Train>,
}

impl Storage {
    /// Create a new storage instance with a temporary file.
    pub fn new_temp() -> Result<Storage, String> {
        let uuid = Uuid::new_v4();
        let file = NamedTempFile::new().map_err(|e| e.to_string())?;
        let db = Database::create(file).map_err(|e| e.to_string())?;

        let cache = Cache::new(100_000);
        Ok(Storage {
            path: None,
            table_name: uuid.to_string(),
            database: db,
            cache,
        })
    }

    /// Create a new storage instance from a specified file path.
    pub fn new_from_path<S: AsRef<str>>(file: S) -> Result<Storage, String> {
        let db = Database::create(file.as_ref())
            .map_err(|e| e.to_string())?;
        let uuid = Uuid::new_v4();

        let cache = Cache::new(100_000);

        let storage = Storage {
            path: Some(file.as_ref().to_string()),
            table_name: uuid.to_string(),
            database: db,
            cache,
        };
        Ok(storage)
    }

    fn table(&self) -> TableDefinition<'_, String, Vec<u8>> {
        TableDefinition::new(&self.table_name)
    }

    pub fn write_train(&mut self, key: TrainId, train: Train) -> Result<(), String> {
        self.cache.insert(key, train.clone());
        self.write(key, train.write_to_vec().unwrap())
    }

    pub fn write_trains(&mut self, trains: Vec<Train>) -> Result<(), String> {
        for train in &trains {
            self.cache.insert(train.id, train.clone());
        }
        self.writes(
            trains
                .into_iter()
                .map(|t| (t.id, t.write_to_vec().unwrap()))
                .collect::<Vec<_>>(),
        )
    }

    pub fn read_train(&self, key: TrainId) -> Option<Train> {
        match self.cache.get(&key) {
            None => Train::read_from_buffer(&self.read_u8(key).ok()?).ok(),
            Some(v) => Some(v),
        }
    }
    /// Write a key-value pair to the storage.
    fn write(&self, key: TrainId, value: Vec<u8>) -> Result<(), String> {
        let mut write_txn = self
            .database
            .begin_write()
            .map_err(|e| e.to_string())?;
        {
            let mut table = write_txn
                .open_table(self.table())
                .map_err(|e| e.to_string())?;
            table
                .insert(key.to_string(), value)
                .map_err(|e| e.to_string())?;
        }
        write_txn.set_durability(Durability::Immediate).map_err(|err| err.to_string())?;
        write_txn
            .commit()
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Write a key-value pair to the storage.
    fn writes(&self, values: Vec<(TrainId, Vec<u8>)>) -> Result<(), String> {
        let mut write_txn = self
            .database
            .begin_write()
            .map_err(|e| e.to_string())?;
        {
            let mut table = write_txn
                .open_table(self.table())
                .map_err(|e| e.to_string())?;
            for (id, value) in values {
                table
                    .insert(id.to_string(), value)
                    .map_err(|e| e.to_string())?;
            }
        }
        write_txn.set_durability(Durability::Immediate).map_err(|err| err.to_string())?;
        write_txn
            .commit()
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Read a value by its key from the storage.
    fn read_u8(&self, key: TrainId) -> Result<Vec<u8>, String> {
        let read_txn = self
            .database
            .begin_read()
            .map_err(|e| e.to_string())?;
        let table = read_txn
            .open_table(self.table())
            .map_err(|e| e.to_string())?;

        let value = table.get(key.to_string());
        let value = value
            .map_err(|e| e.to_string())?
            .map(|entry| entry.value().clone());
        value.ok_or("Key not found".to_string())
    }
    /// Delete a key-value pair from the storage.
    fn delete(&self, key: Value) -> Result<(), String> {
        let delete_txn = self
            .database
            .begin_write()
            .map_err(|e| e.to_string())?;
        {
            let mut table = delete_txn
                .open_table(self.table())
                .map_err(|e| e.to_string())?;
            table
                .remove(key.to_string())
                .map_err(|e| e.to_string())?;
        }
        delete_txn
            .commit()
            .map_err(|e| e.to_string())
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() && let Err(e) = fs::remove_file(path) {
            eprintln!("Failed to remove file: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use value::Value;

    #[test]
    fn test_write_read() {
        let storage = Storage::new_temp().unwrap();
        storage
            .write(
                TrainId::new(0, 0),
                Value::text("David").write_to_vec().unwrap(),
            )
            .unwrap();
        assert_eq!(
            Value::read_from_buffer(&storage.read_u8(TrainId::new(0, 0)).unwrap()).unwrap(),
            Value::text("David")
        );
        assert!(storage.read_u8(TrainId::new(0, 1)).is_err());
    }

    #[test]
    fn test_write_permanently() {
        let path = "db_test";
        {
            let storage = Storage::new_from_path(path).unwrap();
            storage
                .write(
                    TrainId::new(0, 0),
                    Value::text("David").write_to_vec().unwrap(),
                )
                .unwrap();
        }
        assert!(!fs::exists(path).unwrap());
    }
}
