use postgres::{Client, NoTls};
use serde_json::{Map, Number, Value};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PostgresConnection {
    pub url: String,
    pub port: u16,
    pub db: String,
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

    pub async fn connect(&self) -> Result<Client, String> {
        Client::connect(&format!("postgresql://{}@{}:{}", self.db, self.url, self.port), NoTls).await?
    }
}