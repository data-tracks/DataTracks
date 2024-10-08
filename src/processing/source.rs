use std::collections::HashMap;
use std::sync::Arc;

use crate::mqtt::MqttSource;
use crate::processing::plan::SourceModel;
use crate::processing::station::Command;
#[cfg(test)]
use crate::processing::tests::DummySource;
use crate::processing::train::Train;
use crate::ui::ConfigModel;
use crate::util::Tx;
use crossbeam::channel::Sender;
use serde_json::{Map, Value};

pub fn parse_source(type_: &str, options: Map<String, Value>, stop: i64) -> Result<Box<dyn Source>, String> {
    let source: Box<dyn Source> = match type_.to_ascii_lowercase().as_str() {
        "mqtt" => Box::new(MqttSource::parse(stop, options)?),
        #[cfg(test)]
        "dummy" => Box::new(DummySource::parse(stop, options)?),
        _ => Err(format!("Invalid type: {}", type_))?,
    };
    Ok(source)
}

pub trait Source: Send {
    fn parse(stop: i64, options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized;

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;

    fn add_out(&mut self, id: i64, out: Tx<Train>);

    fn get_stop(&self) -> i64;

    fn get_id(&self) -> i64;

    fn get_name(&self) -> String;

    fn dump(&self) -> String;

    fn serialize(&self) -> SourceModel;

    fn from(stop_id: i64, configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
    where
        Self: Sized;

    fn serialize_default() -> Result<SourceModel, ()>
    where
        Self: Sized;

}

