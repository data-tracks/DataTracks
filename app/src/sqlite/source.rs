use crate::processing::Train;
use crate::sqlite::connection::SqliteConnector;
use crate::util::HybridThreadPool;
use core::ConfigModel;
use core::Configurable;
use core::Source;
use rusqlite::params;
use serde_json::{Map, Value};
use std::collections::HashMap;
use error::error::TrackError;
use threading::command::Command::{Ready, Stop};
use threading::multi::MultiSender;

#[derive(Clone)]
pub struct LiteSource {
    connector: SqliteConnector,
    query: String,
}

impl LiteSource {
    pub fn new(path: String, query: String) -> LiteSource {
        let connection = SqliteConnector::new(path.as_str());
        LiteSource {
            connector: connection,
            query,
        }
    }

    fn get_default_configs(&self) -> HashMap<String, ConfigModel> {
        let mut configs = HashMap::new();
        self.connector.serialize(&mut configs);
        configs.insert("query".to_string(), ConfigModel::text(""));
        configs
    }
}

impl Configurable for LiteSource {
    fn name(&self) -> String {
        "SQLite".to_string()
    }

    fn options(&self) -> Map<String, Value> {
        let mut options = Map::new();
        options.insert(String::from("query"), Value::String(self.query.clone()));
        self.connector.add_options(&mut options);
        options
    }
}

impl TryFrom<HashMap<String, ConfigModel>> for LiteSource {
    type Error = String;

    fn try_from(configs: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        let get_config_str = |key: &str| {
            configs
                .get(key)
                .map(|config| config.as_str())
                .ok_or_else(|| format!("Could not get config for '{}'.", key))
        };

        let query = get_config_str("query")?;
        let path = get_config_str("path")?;

        Ok(LiteSource::new(path, query))
    }
}

impl TryFrom<Map<String, Value>> for LiteSource {
    type Error = String;

    fn try_from(options: Map<String, Value>) -> Result<Self, Self::Error> {
        let query = options.get("query").unwrap().as_str().unwrap();
        let path = options.get("path").unwrap().as_str().unwrap();
        Ok(LiteSource::new(path.to_owned(), query.to_owned()))
    }
}

impl Source for LiteSource {
    fn operate(&mut self, id: usize, outs: MultiSender<Train>, pool: HybridThreadPool) -> Result<usize, TrackError> {
        let query = self.query.to_owned();
        let connection = self.connector.clone();

        let control = pool.control_sender();

        pool.execute_async("SQLite Source", move |meta| {
            Box::pin(async move {
                let conn = connection.connect().await.unwrap();
                let mut prepared = conn.prepare_cached(query.as_str()).unwrap();
                control.send(Ready(id))?;
                let count = prepared.column_count();
                loop {
                    if meta.should_stop() {
                        break;
                    }

                    let mut iter = prepared.query(params![]).unwrap();
                    let mut values = vec![];
                    while let Ok(Some(row)) = iter.next() {
                        values.push((row, count).try_into().unwrap());
                    }
                    let train = Train::new_values(values, 0, 0);

                    outs.send(train)?;
                }
                control.send(Stop(id))
            })
        })
    }

    fn type_(&self) -> String {
        String::from("SQLite")
    }

    fn get_configs(&self) -> HashMap<String, ConfigModel> {
        let mut configs = HashMap::new();
        self.connector.serialize(&mut configs);
        configs.insert("query".to_string(), ConfigModel::text(self.query.as_str()));
        configs
    }
}

#[cfg(test)]
mod tests {
    use crate::processing::Plan;

    //#[test]
    fn test_simple_source() {
        let plan = format!(
            "\
            0--1\n\
            In\n\
            Sqlite{{\"path\":\"//test.db\",\"query\":\"SELECT * FROM \\\"user\\\"\"}}:0\n\
            Out\n\
            Dummy{{\"id\": 35, \"result_size\":2}}:1\
            "
        );
        let mut plan = Plan::parse(&plan).unwrap();

        let dummy = plan.get_result(35).clone();
        plan.operate().unwrap();

        let control = plan.control_receiver();

        for _ in 0..4 {
            control.recv().unwrap();
        }
        let values = dummy.lock().unwrap();
        println!("{:?}", values);
    }
}
