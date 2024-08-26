use std::sync::Arc;

use crossbeam::channel::Sender;
use crate::processing::plan::SourceModel;
use crate::processing::station::Command;
use crate::processing::train::Train;
use crate::util::Tx;

pub trait Source: Send {
    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;

    fn add_out(&mut self, id: i64, out: Tx<Train>);

    fn get_stop(&self) -> i64;

    fn get_id(&self) -> i64;

    fn serialize(&self) -> SourceModel;
}

