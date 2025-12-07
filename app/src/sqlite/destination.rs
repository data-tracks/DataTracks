use crate::processing::Train;
use crate::processing::destination::Destination;
use crate::sqlite::connection::SqliteConnector;
use crate::util::DynamicQuery;
use crate::util::{HybridThreadPool, Tx};
use core::ConfigModel;
use core::Configurable;
use rusqlite::params_from_iter;
use serde_json::Map;
use std::collections::HashMap;
use std::time::Duration;
use error::error::TrackError;
use threading::command::Command::Ready;

#[derive(Clone)]
pub struct LiteDestination {
    connector: SqliteConnector,
    query: DynamicQuery,
    path: String,
}

impl LiteDestination {
    pub fn new(path: String, query: String) -> Self {
        let connection = SqliteConnector::new(&path);
        let query = DynamicQuery::build_dynamic_query(query);
        LiteDestination {
            connector: connection,
            query,
            path,
        }
    }
}

impl TryFrom<HashMap<String, ConfigModel>> for LiteDestination {
    type Error = String;

    fn try_from(configs: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        let path = if let Some(path) = configs.get("path") {
            path.as_str()
        } else {
            return Err(String::from("Could not create LiteDestination."));
        };
        let url = if let Some(url) = configs.get("url") {
            url.as_str()
        } else {
            return Err(String::from("Could not create LiteDestination."));
        };

        Ok(LiteDestination::new(path, url.to_string()))
    }
}

impl Configurable for LiteDestination {
    fn name(&self) -> String {
        "SQLite".to_owned()
    }

    fn options(&self) -> Map<String, serde_json::Value> {
        let mut options = Map::new();
        self.connector.add_options(&mut options);
        options.insert(
            String::from("query"),
            serde_json::Value::String(self.query.get_query()),
        );
        options
    }
}

impl Destination for LiteDestination {
    fn parse(options: Map<String, serde_json::Value>) -> Result<Self, TrackError>
    where
        Self: Sized,
    {
        let query = options.get("query").unwrap().as_str().unwrap();
        let path = options.get("path").unwrap().as_str().unwrap();

        let destination = LiteDestination::new(path.to_string(), query.to_owned());

        Ok(destination)
    }

    fn operate(&mut self, id: usize, tx: Tx<Train>, pool: HybridThreadPool) -> Result<usize, TrackError> {
        let query = self.query.clone();
        let path = self.path.clone();
        let rx = tx.subscribe();

        pool.execute_async("SQLite Destination", move |meta| {
            Box::pin(async move {
                let conn = SqliteConnector::new(&path).connect().unwrap();
                let (query, value_functions) = query.prepare_query_transform("$", None, 1)?;

                meta.output_channel.send(Ready(id))?;
                loop {
                    if meta.should_stop() {
                        break;
                    }
                    match rx.try_recv() {
                        Ok(train) => {
                            let values = &train.into_values();
                            if values.is_empty() {
                                continue;
                            }
                            for value in values {
                                let _ = conn
                                    .prepare_cached(&query)
                                    .unwrap()
                                    .query(params_from_iter(value_functions(value)))
                                    .unwrap();
                            }
                        }
                        _ => tokio::time::sleep(Duration::from_nanos(100)).await,
                    }
                }
                Ok(())
            })
        })
    }

    fn type_(&self) -> String {
        String::from("SQLite")
    }

    fn get_configs(&self) -> HashMap<String, core::models::configuration::ConfigModel> {
        let mut configs = HashMap::new();
        self.connector.serialize(&mut configs);
        configs
    }
}

#[cfg(test)]
mod tests {
    use crate::processing::Plan;

    #[test]
    fn test_simple_insert() {
        Plan::parse(
            "\
            0--1\n\
            \n\
            Out\n\
            Sqlite{\"path\": \"local.db\", \"query\": \"INSERT INTO \\\"test_table\\\" VALUES(\\\"$.0\\\", \\\"$.1\\\")\"}:1"
        ).unwrap();
    }
}
