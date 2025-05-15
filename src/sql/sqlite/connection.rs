use crate::ui::{ConfigModel, StringModel};
use rusqlite::Connection;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct SqliteConnector {
    pub path: PathBuf,
}


impl SqliteConnector {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        SqliteConnector { path: path.as_ref().to_path_buf() }
    }

    pub(crate) fn add_options(&self, options: &mut Map<String, Value>) {
        options.insert(String::from("path"), Value::String(self.path.display().to_string()));
    }

    pub(crate) async fn connect(&self) -> Result<Connection, String> {
        Connection::open(format!("sqlite:{:?}", self.path)).map_err(|e| e.to_string())
    }

    pub(crate) fn serialize(&self, configs: &mut HashMap<String, ConfigModel>) {
        configs.insert("path".to_string(), ConfigModel::String(StringModel::new(self.path.display().to_string().as_str())));
    }
}

impl Clone for SqliteConnector {
    fn clone(&self) -> Self {
        SqliteConnector::new(self.path.clone())
    }
}

impl PartialEq for SqliteConnector {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}
