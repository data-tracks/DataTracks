use crate::value::Value;
use redb::{Database, Error, TableDefinition, TypeName};
use tempfile::NamedTempFile;

pub struct Storage<'a>{
    file: NamedTempFile,
    table_name: String,
    table: TableDefinition<'a, String, Value>,
    database: Database,
}

impl<'a> Storage<'a> {
    pub fn new(file: &'a str, table_name: &'a str) -> Result<Storage<'a>, String> {
        let file = NamedTempFile::new().map_err(|e| e.to_string())?;
        let db = Database::create(file.path()).map_err(|e| e.to_string())?;
        Ok(Storage{
            file,
            table_name: table_name.to_string(),
            table: TableDefinition::new(table_name),
            database: db,
        })
    }
    fn write(&self, key: &str, value: &Value) -> Result<(), Error> {

        let write_txn = self.database.begin_write()?;
        {
            let mut table = write_txn.open_table(self.table)?;
            table.insert(key.to_string(), value)?;
        }
        write_txn.commit()?;

        Ok(())
    }

    fn read(&self, key: &str) -> Result<Value, Error> {
        let read_txn = self.database.begin_read()?;
        let table = read_txn.open_table(self.table)?;
        Ok(table.get(key.to_string())?.unwrap().value())
    }
}

impl redb::Value for Value{
    type SelfType<'a> = Value where Self: 'a;
    type AsBytes<'a> = &'a [u8] where Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a
    {
        rkyv::from_bytes(&data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b
    {
        rkyv::to_bytes(&value).unwrap().as_slice()
    }

    fn type_name() -> TypeName {
        TypeName::new("value")
    }
}


#[cfg(test)]
mod tests {
    use crate::util::storage::Storage;

    #[test]
    pub fn test_write() {
        let storage = Storage::new("test", "table").unwrap();
    }
}


