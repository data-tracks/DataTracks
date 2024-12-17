use std::fs::File;
use redb::{Database, Error, TableDefinition, TypeName};
use tempfile::NamedTempFile;
use crate::value::Value;

pub struct Storage<'a>{
    file: NamedTempFile,
    table_name: String,
    table: TableDefinition<'a, &'a str, Value>,
    database: &'a Database,
}

impl Storage {
    pub fn new(file: &str, table_name: &str) -> Result<Storage, String> {
        let file = NamedTempFile::new().map_err(|e| e.to_string())?;
        let db = Database::create(file.path()).map_err(|e| e.to_string())?;
        Ok(Storage{
            file,
            table_name: table_name.to_string(),
            table: TableDefinition::new(table_name),
            database: &db,
        })
    }
    fn write(&self, key: &str, value: &Value) -> Result<(), Error> {

        let write_txn = self.database.begin_write()?;
        {
            let mut table = write_txn.open_table(self.table)?;
            table.insert(key, value)?;
        }
        write_txn.commit()?;

        Ok(())
    }

    fn read(&self, key: &str) -> Result<Value, Error> {
        let read_txn = self.database.begin_read()?;
        let table = read_txn.open_table(self.table)?;
        Ok(table.get(key)?.unwrap().value())
    }
}

impl redb::Value for Value{
    type SelfType<'a>
    where
        Self: 'a = Value;
    type AsBytes<'a>
    where
        Self: 'a = ();

    fn fixed_width() -> Option<usize> {
        todo!()
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a
    {
        todo!()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b
    {
        todo!()
    }

    fn type_name() -> TypeName {
        todo!()
    }
}


