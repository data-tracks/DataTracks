use std::sync::Arc;

use crossbeam::channel::Sender;

use crate::processing::station::Command;
use crate::processing::train::Train;

pub trait Destination: Send {
    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;
    fn get_in(&self) -> Sender<Train>;

    fn get_stop(&self) -> i64;

    fn get_id(&self) -> i64;
}