use crate::algebra::Algebra;
use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::ui::{ConfigModel, StringModel};
use crate::util::{new_channel, Rx, Tx, GLOBAL_ID};
use crossbeam::channel::Sender;
use regex::Regex;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub struct LiteDestination {
    id: i64,
    receiver: Rx<Train>,
    sender: Tx<Train>,
    path: String,
    query: fn(Vec<Value>) -> String,
}

impl LiteDestination {
    pub fn new(path: String, query: String) -> LiteDestination {
        let (tx, _num, rx) = new_channel();
        let query = extract_dynamic_parameters(query);
        LiteDestination { id: GLOBAL_ID.new_id(), receiver: rx, sender: tx, path, query }
    }
}

fn extract_dynamic_parameters(query: String) -> fn(Vec<Value>) -> String {
    let mut parts = query.split("?").collect::<Vec<&str>>();

    |values: Vec<Value>| -> String {
        let mut query = parts.pop().unwrap().to_string();
        for value in values {
            query += &value.to_string();
            query += parts.pop().unwrap_or("");
        }
        query
    }

}

impl Configurable for LiteDestination {
    fn get_name(&self) -> String {
        "SQLite".to_owned()
    }

    fn get_options(&self) -> Map<String, Value> {
        let mut options = Map::new();
        options.insert(String::from("path"), Value::String(self.path.clone()));
        options
    }
}

impl Destination for LiteDestination {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        let query = options.get("query").unwrap().as_str().unwrap();
        let path = options.get("path").unwrap().as_str().unwrap();

        let destination = LiteDestination::new(path.to_string(), query.to_owned());

        Ok(destination)
    }

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        todo!()
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
            SQLite{{path: \"local.db\", query: \"INSERT INTO test_table VALUES(?, ?)\"}}:1"
        ).unwrap();
    }
}