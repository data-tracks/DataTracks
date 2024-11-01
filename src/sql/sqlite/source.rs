use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::station::Command::Ready;
use crate::processing::{plan, Train};
use crate::ui::ConfigModel;
use crate::util::{Tx, GLOBAL_ID};
use crate::value::value;
use crossbeam::channel::{unbounded, Sender};
use serde_json::{Map, Value};
use sqlx::{Connection, Database, Row, SqliteConnection, ValueRef};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct LiteSource {
    id: i64,
    path: String,
    outs: Vec<Tx<Train>>,
    query: String,
}

impl LiteSource {
    pub fn new(path: String, query: String) -> LiteSource {
        let id = GLOBAL_ID.new_id();
        LiteSource { id, path, outs: Vec::new(), query }
    }
}

impl Configurable for LiteSource {
    fn get_name(&self) -> String {
        "SQLite".to_string()
    }

    fn get_options(&self) -> Map<String, Value> {
        let mut options = Map::new();
        options.insert(String::from("query"), Value::String(self.query.clone()));
        options.insert(String::from("path"), Value::String(self.path.clone()));
        options
    }
}

impl Source for LiteSource {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        let query = options.get("query").unwrap().as_str().unwrap();
        let path = options.get("path").unwrap().as_str().unwrap();
        Ok(LiteSource::new(path.to_owned(), query.to_owned()))
    }

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        let (tx, rx) = unbounded();
        let id = self.id.clone();
        let query = self.query.to_owned();
        let runtime = Runtime::new().unwrap();
        let path = self.path.clone();
        let sender = self.outs.clone();

        runtime.block_on(async {
            let mut conn = SqliteConnection::connect(&format!("sqlite::{}", path)).await.unwrap();
            control.send(Ready(id)).unwrap();
            loop {
                if plan::check_commands(&rx) { break; }

                let mut query = sqlx::query_as(query.as_str());
                let values: Vec<value::Value> = query.fetch_all(&mut conn).await.unwrap();
                let train = Train::new(-1, values);

                for sender in &sender {
                    sender.send(train.clone()).unwrap();
                }

            }
        });
        tx
    }

    fn get_outs(&mut self) -> &mut Vec<Tx<Train>> {
        &mut self.outs
    }


    fn get_id(&self) -> i64 {
        self.id
    }

    fn serialize(&self) -> SourceModel {
        let mut configs = HashMap::new();
        configs.insert("path".to_string(), ConfigModel::text(self.path.as_str()));
        configs.insert("query".to_string(), ConfigModel::text(self.query.as_str()));
        SourceModel { type_name: String::from("SQLite"), id: self.id.to_string(), configs }
    }

    fn from(configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
    where
        Self: Sized,
    {
        let query = if let Some(query) = configs.get("query") {
            query.as_str()
        } else {
            return Err(String::from("Could not create SQLiteSource."))
        };
        let path = if let Some(path) = configs.get("path") {
            path.as_str()
        } else {
            return Err(String::from("Could not create MqttSource."))
        };

        Ok(Box::new(LiteSource::new(path, query)))
    }

    fn serialize_default() -> Result<SourceModel, ()>
    where
        Self: Sized,
    {
        Ok(SourceModel { type_name: String::from("SQLite"), id: String::from("SQLite"), configs: HashMap::new() })
    }
}


