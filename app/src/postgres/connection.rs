use core::ConfigModel;
use postgres::{Client, NoTls};
use serde_json::{Map, Number, Value};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
pub struct PostgresConnection {
    pub(crate) url: String,
    pub(crate) port: u16,
    pub(crate) db: String,
    pub(crate) user: String,
}

impl PostgresConnection {
    pub fn new<S1: AsRef<str>, S2: AsRef<str>, S3: AsRef<str>>(url: S1, port: u16, db: S2, user: S3) -> Self {
        PostgresConnection {
            url: url.as_ref().to_string(),
            port,
            db: db.as_ref().to_string(),
            user: user.as_ref().to_string(),
        }
    }

    pub(crate) fn add_options(&self, options: &mut Map<String, Value>) {
        options.insert(String::from("url"), Value::String(self.url.clone()));
        options.insert(String::from("port"), Value::Number(Number::from(self.port)));
        options.insert(String::from("database"), Value::String(self.db.clone()));
        options.insert(String::from("user"), Value::String(self.user.clone()));
    }

    pub(crate) fn serialize(&self) -> HashMap<String, ConfigModel> {
        let mut map = HashMap::new();
        map.insert(String::from("url"), ConfigModel::text(&self.url.clone()));
        map.insert(String::from("port"), ConfigModel::number(self.port as i64));
        map.insert(String::from("database"), ConfigModel::text(&self.db.clone()));
        map.insert(String::from("user"), ConfigModel::text(&self.user.clone()));

        map
    }

    pub fn connect(&self) -> Result<Client, String> {
        Client::connect(
            &format!("dbname={db} host={host} port={port} user={user}", db=self.db, host=self.url, port=self.port, user=self.user),
            NoTls,
        )
        .map_err(|e| e.to_string())
    }
}

