use cdc::{ChangeDataCapture, MongoDbCdc, MongoIdentifier};
use core::ConfigModel;
use core::Source;
use serde_json::{Map, Value};
use std::collections::HashMap;
use threading::multi::MultiSender;
use threading::pool::HybridThreadPool;
use value::train::Train;

#[derive(Clone)]
pub struct MongoDbSource {
    url: String,
    port: u16,
    entity: MongoIdentifier,
}

impl MongoDbSource {
    pub fn new<S: AsRef<str>>(url: S, port: u16, entity: MongoIdentifier) -> Self {
        MongoDbSource {
            url: url.as_ref().to_string(),
            port,
            entity,
        }
    }
}

impl TryFrom<HashMap<String, ConfigModel>> for MongoDbSource {
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

        let schema = configs.get("database").map(|s| s.as_str());
        let collection = configs.get("collection").map(|s| s.as_str());

        let entity = MongoIdentifier::new(schema, collection);

        Ok(MongoDbSource::new(url, port, entity))
    }
}

impl TryFrom<Map<String, Value>> for MongoDbSource {
    type Error = String;

    fn try_from(options: Map<String, Value>) -> Result<Self, Self::Error> {
        let url = options.get("url").unwrap().as_str().unwrap();
        let port = options.get("port").unwrap().as_i64().unwrap() as u16;
        let database = options
            .get("database")
            .map(|s| s.as_str())
            .flatten()
            .map(|s| s.to_owned());
        let collection = options
            .get("collection")
            .map(|s| s.as_str())
            .flatten()
            .map(|s| s.to_owned());

        let entity = MongoIdentifier::new(database, collection);

        Ok(MongoDbSource::new(url, port.to_owned(), entity))
    }
}

impl core::processing::configuration::Configurable for MongoDbSource {
    fn name(&self) -> String {
        "MongoDb".to_string()
    }

    fn options(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("url".to_string(), serde_json::to_value(&self.url).unwrap());
        map.insert(
            "port".to_string(),
            serde_json::to_value(&self.port).unwrap(),
        );

        if let Some(database) = self.entity.database.clone() {
            map.insert(
                "database".to_string(),
                serde_json::to_value(database).unwrap(),
            );
        }

        if let Some(collection) = self.entity.collection.clone() {
            map.insert(
                "collection".to_string(),
                serde_json::to_value(collection).unwrap(),
            );
        }

        map
    }
}

impl Source for MongoDbSource {
    fn operate(&mut self, id: usize, outs: MultiSender<Train>, pool: HybridThreadPool) -> Result<usize, String> {
        let mut cdc = MongoDbCdc::new(self.url.clone(), self.port, self.entity.clone())?;

        cdc.listen(id, outs, pool)
    }

    fn type_(&self) -> String {
        "MongoDb".to_owned()
    }

    fn get_configs(&self) -> HashMap<String, ConfigModel> {
        let mut configs = HashMap::new();
        configs.insert("url".to_string(), ConfigModel::text(self.url.as_str()));
        configs.insert("port".to_string(), ConfigModel::number(self.port as i64));
        configs.insert(
            "database".to_string(),
            ConfigModel::text(self.entity.database.clone().unwrap_or_default()),
        );
        configs.insert(
            "collection".to_string(),
            ConfigModel::text(self.entity.collection.clone().unwrap_or_default()),
        );
        configs
    }
}
