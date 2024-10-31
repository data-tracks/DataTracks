use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::ui::ConfigModel;
use crate::util::Tx;
use crossbeam::channel::Sender;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub struct LiteSource {
    id: i64,
    outs: Vec<Tx<Train>>,
    query: String,
}

impl LiteSource {}

impl Configurable for LiteSource {
    fn get_name(&self) -> String {
        "SQLite".to_string()
    }

    fn get_options(&self) -> Map<String, Value> {
        todo!()
    }
}

impl Source for LiteSource {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        todo!()
    }

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        todo!()
    }

    fn get_outs(&mut self) -> &mut Vec<Tx<Train>> {
        &mut self.outs
    }


    fn get_id(&self) -> i64 {
        self.id
    }

    fn serialize(&self) -> SourceModel {
        todo!()
    }

    fn from(configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
    where
        Self: Sized,
    {
        todo!()
    }

    fn serialize_default() -> Result<SourceModel, ()>
    where
        Self: Sized,
    {
        todo!()
    }
}