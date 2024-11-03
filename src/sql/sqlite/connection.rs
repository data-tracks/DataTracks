use crate::ui::{ConfigModel, StringModel};
use serde_json::{Map, Value};
use sqlx::{Connection, SqliteConnection};
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct SqliteConnector {
    pub path: String,
}


impl SqliteConnector {
    pub fn new(path: &str) -> Self {
        SqliteConnector { path: path.to_string() }
    }

    pub(crate) fn add_options(&self, options: &mut Map<String, Value>) {
        options.insert(String::from("path"), Value::String(self.path.clone()));
    }

    pub(crate) async fn connect(&self) -> Result<SqliteConnection, String> {
        SqliteConnection::connect(&format!("sqlite:{}", self.path)).await.map_err(|e| e.to_string())
    }

    pub(crate) fn serialize(&self, configs: &mut HashMap<String, ConfigModel>) {
        configs.insert("path".to_string(), ConfigModel::String(StringModel::new(&self.path)));
    }
}

impl Clone for SqliteConnector {
    fn clone(&self) -> Self {
        SqliteConnector::new(self.path.as_str())
    }
}

impl PartialEq for SqliteConnector {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}
