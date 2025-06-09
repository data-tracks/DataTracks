use postgres::{Client, NoTls};
use serde_json::{Map, Number, Value};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PostgresConnection {
    pub(crate) url: String,
    pub(crate) port: u16,
    pub(crate) db: String,
}

impl PostgresConnection {
    pub fn new(url: String, port: u16, db: String) -> Self {
        PostgresConnection { url, port, db }
    }

    pub(crate) fn add_options(&self, options: &mut Map<String, Value>) {
        options.insert(String::from("url"), Value::String(self.url.clone()));
        options.insert(String::from("port"), Value::Number(Number::from(self.port)));
        options.insert(String::from("db"), Value::String(self.db.clone()));
    }

    pub fn connect(&self) -> Result<Client, String> {
        Client::connect(
            &format!("postgresql://{}@{}:{}", self.db, self.url, self.port),
            NoTls,
        )
        .map_err(|e| e.to_string())
    }
}
