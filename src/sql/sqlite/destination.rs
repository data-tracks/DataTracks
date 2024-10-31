use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::util::{new_channel, Rx, Tx, GLOBAL_ID};
use crossbeam::channel::Sender;
use serde_json::{Map, Value};
use std::sync::Arc;
use crate::algebra::AlgebraType;

pub struct LiteDestination {
    id: i64,
    receiver: Rx<Train>,
    sender: Tx<Train>,
    query: String,
    algebra: AlgebraType
}

impl LiteDestination {
    pub fn new() -> LiteDestination {
        let (tx, _num, rx) = new_channel();
        LiteDestination { id: GLOBAL_ID.new_id(), receiver: rx, sender: tx }
    }
}

impl Configurable for LiteDestination {
    fn get_name(&self) -> String {
        "SQLite".to_owned()
    }

    fn get_options(&self) -> Map<String, Value> {
        todo!()
    }
}

impl Destination for LiteDestination {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        todo!()
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
        todo!()
    }

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized,
    {
        todo!()
    }
}