use std::collections::HashMap;
use std::sync::Arc;

use crate::mqtt::MqttSource;
use crate::processing::plan::SourceModel;
use crate::processing::station::Command;
use crate::processing::train::Train;
use crate::processing::DummySource;
use crate::ui::ConfigModel;
use crate::util::Tx;
use crossbeam::channel::Sender;
use serde_json::{Map, Value};

pub fn parse_source(type_: &str, options: Map<String, Value>, stop: i64) -> Result<Box<dyn Source>, String> {
    match type_.to_ascii_lowercase().as_str() {
        "mqtt" => MqttSource::parse(stop, options),
        "dummy" => DummySource::parse(stop, options),
        _ => Err(format!("Invalid type: {}", type_)),
    }
}

pub trait Source: Send {
    fn parse(stop: i64, options: Map<String, Value>) -> Result<Box<dyn Source>, String>;

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;

    fn add_out(&mut self, id: i64, out: Tx<Train>);

    fn get_stop(&self) -> i64;

    fn get_id(&self) -> i64;

    fn serialize(&self) -> SourceModel;

    fn from(stop_id: i64, configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
    where
        Self: Sized;

    fn serialize_default() -> Result<SourceModel, ()>
    where
        Self: Sized;

}

