use std::sync::Arc;

use crate::mqtt::MqttDestination;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
#[cfg(test)]
use crate::processing::tests::DummyDestination;
use crate::processing::train::Train;
use crate::util::Tx;
use crossbeam::channel::Sender;
use serde_json::{Map, Value};

pub fn parse_destination(type_: &str, options: Map<String, Value>, stop: i64) -> Result<Box<dyn Destination>, String> {
    let destination: Box<dyn Destination> = match type_.to_ascii_lowercase().as_str() {
        "mqtt" => Box::new(MqttDestination::parse(stop, options)?),
        #[cfg(test)]
        "dummy" => Box::new(DummyDestination::parse(stop, options)?),
        _ => Err(format!("Invalid type: {}", type_))?,
    };
    Ok(destination)
}

pub trait Destination: Send {
    fn parse(stop: i64, options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized;

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;
    fn get_in(&self) -> Tx<Train>;

    fn get_stop(&self) -> i64;

    fn get_id(&self) -> i64;

    fn get_name(&self) -> String;

    fn dump(&self) -> String;

    fn serialize(&self) -> DestinationModel;

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized;
}