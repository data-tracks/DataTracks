use crate::algebra::Algebra;
use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::station::Command::{Ready, Stop};
use crate::processing::Train;
use crate::ui::{ConfigModel, StringModel};
use crate::util::{new_channel, Rx, Tx, GLOBAL_ID};
use crate::value::Value;
use crossbeam::channel::{unbounded, Sender};
use serde_json::Map;
use sqlx::{Connection, Executor, SqliteConnection};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;

pub struct LiteDestination {
    id: i64,
    receiver: Rx<Train>,
    sender: Tx<Train>,
    path: String,
    query: String,
}

impl LiteDestination {
    pub fn new(path: String, query: String) -> LiteDestination {
        let (tx, _num, rx) = new_channel();
        LiteDestination { id: GLOBAL_ID.new_id(), receiver: rx, sender: tx, path, query }
    }
}



impl Configurable for LiteDestination {
    fn get_name(&self) -> String {
        "SQLite".to_owned()
    }

    fn get_options(&self) -> Map<String, serde_json::Value> {
        let mut options = Map::new();
        options.insert(String::from("path"), serde_json::Value::String(self.path.clone()));
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

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        let receiver = self.receiver.clone();
        let (tx, rx) = unbounded();
        let id = self.id.clone();
        let query = self.query.to_owned();
        let runtime = Runtime::new().unwrap();
        let path = self.path.clone();


        thread::spawn(move || {
            runtime.block_on(async {
                let mut conn = SqliteConnection::connect(&format!("sqlite::{}", path)).await.unwrap();
                control.send(Ready(id)).unwrap();
                loop {
                    if let Ok(command) = rx.try_recv() {
                        match command {
                            Stop(_) => break,
                            _ => {}
                        }
                    }
                    match receiver.try_recv() {
                        Ok(mut train) => {
                            let values = train.values.take().unwrap();
                            if values.is_empty() {
                                continue;
                            }
                            for value in values {
                                let mut query = sqlx::query(query.as_str());
                                match value {
                                    Value::Array(a) => {
                                        for val in a.0 {
                                            query = query.bind(val.to_string());
                                        }
                                    }
                                    val => {query = query.bind(val.to_string());}
                                }
                                query.execute(&mut conn).await.unwrap();
                            }
                        }
                        _ => tokio::time::sleep(Duration::from_nanos(100)).await
                    }
                }
            })
        });
        tx
    }

    fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }

    fn get_id(&self) -> i64 {
        self.id.clone()
    }

    fn serialize(&self) -> DestinationModel {
        let mut configs = HashMap::new();
        configs.insert("path".to_string(), ConfigModel::String(StringModel::new(&self.path)));
        DestinationModel { type_name: self.get_name(), id: self.id.to_string(), configs }
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

    fn test_simple_insert() {
        let plan = Plan::parse(
            "\
            0--1\n\
            \n\
            Out\n\
            SQLite{{path: \"local.db\", query: \"INSERT INTO test_table VALUES($.0, $.1)\"}}:1"
        ).unwrap();
    }
}