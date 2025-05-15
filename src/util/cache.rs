use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use rusqlite::Connection;
use rusqlite::fallible_iterator::FallibleIterator;
use serde::Serialize;
use crate::sql;
use crate::sql::SqliteConnector;

#[derive(Clone)]
pub struct Storage<Key, Element> {
    values: StorageStrategy<Key, Element>,
    order: VecDeque<Key>,
    max: usize,
}

impl<Key: Ord, Element: Serialize> Storage<Key, Element> {
    pub fn new(max: usize) -> Storage<Key, Element> {
        Storage {values: StorageStrategy::memory(), order: VecDeque::new(), max}
    }

    pub fn get(&self, key: &Key) -> Option<&Element> {
        self.values.get(key)
    }

    pub fn set(&mut self, key: Key, element: Element) {
        self.values.set(key, element);
        if self.order.len() > self.max {
            self.order.pop_front();
        }
    }

    pub fn remove(&mut self, key: &Key) -> Option<Element> {
        self.values.remove(key)
    }
}


pub enum StorageStrategy<Key, Element> {
    Memory(BTreeMap<Key, Element>),
    Sqlite(SqliteStorage<Key, Element>)
}

impl<Key:Ord, Element: Serialize> StorageStrategy<Key, Element> {
    pub fn memory() -> Self<Key, Element> {
        StorageStrategy::Memory(BTreeMap::new())
    }

    pub fn get(&self, key: &Key) -> Option<&Element> {
        match self {
            StorageStrategy::Memory(m) => m.get(key),
            StorageStrategy::Sqlite(s) => s.get(key)
        }
    }

    pub fn set(&mut self, key: Key, element: Element) {
        match self {
            StorageStrategy::Memory(m) => {m.insert(key, element);},
            StorageStrategy::Sqlite(l) => l.set(key, element)
        }
    }

    pub fn remove(&mut self, key: &Key) -> Option<Element> {
        match self {
            StorageStrategy::Memory(m) => m.remove(key),
            StorageStrategy::Sqlite(s) => s.remove()
        }
    }
}

pub struct SqliteStorage<Key, Element> {
    connection: SqliteConnector,
    connected: Connection
}

impl<Key:Ord, Element: Serialize> SqliteStorage<Key, Element> {

    pub fn new<P: AsRef<Path>>(path: P) -> Self<Key, Element> {
        let connection = SqliteConnector::new(path);
        let connected = connection.connect();
        SqliteStorage { connection, connected  }
    }
    pub fn get(&self, key: &Key) -> Option<&Element> {
        todo!()
    }

    pub fn set(&mut self, key: Key, element: Element) {
        todo!()
    }

    pub fn remove(&mut self, key: &Key) -> Option<Element> {
        todo!()
    }
}