use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::station::Command::{Ready, Stop};
use crate::processing::{plan, Train};
use crate::sql::sqlite::connection::SqliteConnector;
use crate::ui::ConfigModel;
use crate::util::{new_id, Tx};
use crossbeam::channel::{unbounded, Sender};
use rusqlite::params;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct LiteSource {
    id: usize,
    connector: SqliteConnector,
    outs: Vec<Tx<Train>>,
    query: String,
}

impl LiteSource {
    pub fn new(path: String, query: String) -> LiteSource {
        let id = new_id();
        let connection = SqliteConnector::new(path.as_str());
        LiteSource { id, connector: connection, outs: Vec::new(), query }
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
        let id = self.id;
        let query = self.query.to_owned();
        let runtime = Runtime::new().unwrap();
        let connection = self.connector.clone();
        let sender = self.outs.clone();

        runtime.block_on(async {
            let conn = connection.connect().await.unwrap();
            let mut prepared = conn.prepare_cached(query.as_str()).unwrap();
            control.send(Ready(id)).unwrap();
            let count = prepared.column_count();
            loop {
                if plan::check_commands(&rx) { break; }

                let mut iter = prepared.query(params![]).unwrap();
                let mut values = vec![];
                while let Ok(Some(row)) = iter.next() {
                    values.push((row, count).try_into().unwrap());
                }
                let train = Train::new(values);

                for sender in &sender {
                    sender.send(train.clone()).unwrap();
                }

            }
            control.send(Stop(id)).unwrap();
        });
        tx
    }

    fn outs(&mut self) -> &mut Vec<Tx<Train>> {
        &mut self.outs
    }


    fn id(&self) -> usize {
        self.id
    }

    fn type_(&self) -> String {
        String::from("SQLite")
    }

    fn serialize(&self) -> SourceModel {
        let mut configs = HashMap::new();
        self.connector.serialize(&mut configs);
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

        for _ in 0..4 {
            plan.control_receiver.1.recv().unwrap();
        }
        let values = dummy.lock().unwrap();
        println!("{:?}", values);
    }
}



