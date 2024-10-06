use std::sync::Arc;

use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::train::Train;
use crate::util::Tx;
use crossbeam::channel::Sender;
use serde_json::{Map, Value};

pub trait Destination: Send {
    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;
    fn get_in(&self) -> Tx<Train>;

    fn get_stop(&self) -> i64;

    fn get_id(&self) -> i64;

    fn serialize(&self) -> DestinationModel;

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized;

    fn parse(type_: &str, options: Map<String, Value>) -> Result<Box<dyn Destination>, String> {
        match type_.to_ascii_lowercase().as_str() {
            "mqtt" => {}
            "dummy" => {}
            _ => {
                Err(format!("Invalid type: {}", type_))
            }
        }
    }
}