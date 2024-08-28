use std::sync::Arc;

use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::train::Train;
use crate::util::Tx;
use crossbeam::channel::Sender;

pub trait Destination: Send {
    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;
    fn get_in(&self) -> Tx<Train>;

    fn get_stop(&self) -> i64;

    fn get_id(&self) -> i64;

    fn serialize(&self) -> DestinationModel;

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized;
}