use std::sync::Arc;

use crate::mqtt::MqttDestination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
#[cfg(test)]
use crate::processing::tests::DummyDestination;
use crate::processing::train::Train;
use crate::sql::LiteDestination;
use crate::util::Tx;
use crossbeam::channel::Sender;
use serde_json::{Map, Value};
#[cfg(test)]
use std::sync::Mutex;
use crate::http::destination::HttpDestination;

pub fn parse_destination(type_: &str, options: Map<String, Value>) -> Result<Box<dyn Destination>, String> {
    let destination: Box<dyn Destination> = match type_.to_ascii_lowercase().as_str() {
        "mqtt" => Box::new(MqttDestination::parse(options)?),
        "sqlite" => Box::new(LiteDestination::parse(options)?),
        "http" => Box::new(HttpDestination::parse(options)?),
        #[cfg(test)]
        "dummy" => Box::new(DummyDestination::parse(options)?),
        _ => Err(format!("Invalid type: {}", type_))?,
    };
    Ok(destination)
}

pub trait Destination: Send + Configurable + Sync {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized;

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;
    fn get_in(&self) -> Tx<Train>;

    fn get_id(&self) -> usize;

    fn dump_destination(&self, _include_id: bool) -> String {
        Configurable::dump(self)
    }

    fn serialize(&self) -> DestinationModel;

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized;


    #[cfg(test)]
    fn get_result_handle(&self) -> Arc<Mutex<Vec<Train>>> {
        panic!()
    }
}