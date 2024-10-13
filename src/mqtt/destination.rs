use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
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

impl Configurable for MqttDestination {
    fn get_options(&self) -> Map<String, Value> {
        Map::new()
    }

    fn get_name(&self) -> String {
        String::from("Mqtt")
    }
}

impl Destination for MqttDestination {
    fn parse(_stop: i64, _options: Map<String, Value>) -> Result<Self, String> {
        todo!()
    }

    fn operate(&mut self, _control: Arc<Sender<Command>>) -> Sender<Command> {
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