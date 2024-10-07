use crate::processing::destination::Destination;
use crate::processing::plan::DestinationModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::util::Tx;
use crossbeam::channel::Sender;
use serde_json::{Map, Value};
use std::sync::Arc;

pub struct MqttDestination {
    port: u16,
}

impl MqttDestination {
    pub fn new(port: u16) -> Self {
        MqttDestination { port }
    }
}

impl Destination for MqttDestination {
    fn parse(stop: i64, options: Map<String, Value>) -> Result<Box<dyn Source>, String> {
        todo!()
    }

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        todo!()
    }

    fn get_in(&self) -> Tx<Train> {
        todo!()
    }

    fn get_stop(&self) -> i64 {
        todo!()
    }

    fn get_id(&self) -> i64 {
        todo!()
    }

    fn serialize(&self) -> DestinationModel {
        todo!()
    }

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized,
    {
        todo!()
    }
}