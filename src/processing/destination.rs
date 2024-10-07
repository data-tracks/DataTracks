use std::sync::Arc;

use crate::mqtt::MqttDestination;
use crate::processing::plan::DestinationModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::train::Train;
use crate::processing::DummyDestination;
use crate::util::Tx;
use crossbeam::channel::Sender;
use serde_json::{Map, Value};

fn parse_destination(type_: &str, options: Map<String, Value>, stop: i64) -> Result<Box<dyn Destination>, String> {
    match type_.to_ascii_lowercase().as_str() {
        "mqtt" => MqttDestination::parse(stop, options),
        "dummy" => DummyDestination::parse(stop, options),
        _ => Err(format!("Invalid type: {}", type_)),
    }
}

pub trait Destination: Send {
    fn parse(stop: i64, options: Map<String, Value>) -> Result<Box<dyn Source>, String>;

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;
    fn get_in(&self) -> Tx<Train>;

    fn get_stop(&self) -> i64;

    fn get_id(&self) -> i64;

    fn serialize(&self) -> DestinationModel;

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized;
}