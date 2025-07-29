use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command::Ready;
use crate::processing::{plan, Train};
use crate::sql::sqlite::connection::SqliteConnector;
use crate::util::{new_channel, new_id, DynamicQuery};
use crate::util::{HybridThreadPool, Tx};
use rusqlite::params_from_iter;
use serde_json::Map;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone)]
pub struct LiteDestination {
    id: usize,
    sender: Tx<Train>,
    connector: SqliteConnector,
    query: DynamicQuery,
    path: String,
}

impl LiteDestination {
    pub fn new(path: String, query: String) -> Self {
        let (tx, _) = new_channel("SQLite Destination", false);
        let connection = SqliteConnector::new(&path);
        let query = DynamicQuery::build_dynamic_query(query);
        LiteDestination {
            id: new_id(),
            sender: tx,
            connector: connection,
            query,
            path
        }
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
    fn parse(options: Map<String, serde_json::Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        let query = options.get("query").unwrap().as_str().unwrap();
        let path = options.get("path").unwrap().as_str().unwrap();

        let destination = LiteDestination::new(path.to_string(), query.to_owned());

        Ok(destination)
    }

    fn operate(
        &mut self,
        pool: HybridThreadPool,
    ) -> usize {
        let receiver = self.sender.subscribe();
        let id = self.id;
        let query = self.query.clone();
        let path = self.path.clone();

        //let connection = self.connector.clone();

        pool.execute_async( "SQLite Destination".to_string(),move |meta| {
            Box::pin( async move {
                    let conn = SqliteConnector::new(&path).connect().await.unwrap();
                    let (query, value_functions) = query.prepare_query("$", None);

                    meta.output_channel.send(Ready(id));
                    loop {
                        if plan::check_commands(&meta.ins.1) {
                            break;
                        }
                        match receiver.try_recv() {
                            Ok(train) => {
                                let values = &train.values;
                                if values.is_empty() {
                                    continue;
                                }
                                for value in values {
                                    let _ = conn.prepare_cached(&query).unwrap()
                                        .query(params_from_iter(value_functions(value)))
                                        .unwrap();
                                }
                            }
                            _ => tokio::time::sleep(Duration::from_nanos(100)).await,
                        }
                    }
                })}, vec![])
    }

    fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }

    fn id(&self) -> usize {
        self.id
    }

    fn type_(&self) -> String {
        String::from("SQLite")
    }

    fn serialize(&self) -> DestinationModel {
        let mut configs = HashMap::new();
        self.connector.serialize(&mut configs);
        DestinationModel {
            type_name: self.name(),
            id: self.id.to_string(),
            configs,
        }
    }

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized,
    {
        None
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
