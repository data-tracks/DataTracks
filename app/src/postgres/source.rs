use crate::util::HybridThreadPool;
use cdc::ChangeDataCapture;
use cdc::{PostgresCdc, PostgresIdentifier};
use core::ConfigModel;
use core::Configurable;
use core::Source;
use serde_json::{Map, Value};
use std::collections::HashMap;
use error::error::TrackError;
use threading::multi::MultiSender;
use value::train::Train;

#[derive(Clone)]
pub struct PostgresSource {
    url: String,
    port: u16,
    table: Option<PostgresIdentifier>,
}

impl PostgresSource {
    pub fn new(url: String, port: u16, table: Option<PostgresIdentifier>) -> Self {
        Self { url, port, table }
    }
}

impl Configurable for PostgresSource {
    fn name(&self) -> String {
        "Postgres".to_owned()
    }

    fn options(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("url".to_string(), serde_json::to_value(&self.url).unwrap());
        map.insert(
            "port".to_string(),
            serde_json::to_value(&self.port).unwrap(),
        );
        if let Some(table) = &self.table {
            map.insert(
                "schema".to_string(),
                serde_json::to_value(table.schema.clone().unwrap_or_default()).unwrap(),
            );
            map.insert(
                "table".to_string(),
                serde_json::to_value(table.table.clone()).unwrap(),
            );
        }

        map
    }
}

impl TryFrom<HashMap<String, ConfigModel>> for PostgresSource {
    type Error = String;

    fn try_from(configs: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        let url = configs
            .get("url")
            .map(|config| config.as_str())
            .ok_or("Could not get config url".to_string())?;
        let port = configs
            .get("port")
            .map(|config| config.as_str().parse::<u16>().unwrap())
            .ok_or("Could not get config port".to_string())?;

        let schema = configs.get("schema").map(|s| s.as_str());
        let table = configs.get("table").map(|s| s.as_str());

        let table = table.map(|t| PostgresIdentifier::new(schema, t));

        Ok(PostgresSource::new(url, port, table))
    }
}

impl TryFrom<Map<String, Value>> for PostgresSource {
    type Error = String;

    fn try_from(options: Map<String, Value>) -> Result<Self, Self::Error> {
        let url = options.get("url").unwrap().as_str().unwrap();
        let port = options.get("port").unwrap().as_i64().unwrap() as u16;
        let schema = options
            .get("schema")
            .map(|s| s.as_str())
            .flatten()
            .map(|s| s.to_owned());
        let table = options
            .get("table")
            .map(|s| s.as_str())
            .flatten()
            .map(|s| s.to_owned());

        let table = table.map(|t| PostgresIdentifier::new(schema, t));

        Ok(PostgresSource::new(url.to_owned(), port.to_owned(), table))
    }
}

impl Source for PostgresSource {
    fn operate(&mut self, id: usize, outs: MultiSender<Train>, pool: HybridThreadPool) -> Result<usize, TrackError> {
        let mut cdc = PostgresCdc::new(
            self.url.clone(),
            self.port,
            self.table.clone(),
        )?;

        cdc.listen(id, outs, pool)
    }

    fn type_(&self) -> String {
        "Postgres".to_owned()
    }

    fn get_configs(&self) -> HashMap<String, ConfigModel> {
        let mut configs = HashMap::new();
        configs.insert("url".to_string(), ConfigModel::text(self.url.as_str()));
        configs.insert("port".to_string(), ConfigModel::number(self.port as i64));
        configs.insert(
            "schema".to_string(),
            ConfigModel::text(
                self.table
                    .clone()
                    .map(|t| t.schema)
                    .flatten()
                    .unwrap_or_default(),
            ),
        );
        configs.insert(
            "table".to_string(),
            ConfigModel::text(self.table.clone().map(|t| t.table).unwrap_or_default()),
        );
        configs
    }
}
