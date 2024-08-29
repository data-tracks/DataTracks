use std::collections::HashMap;
use std::sync::Arc;

use crate::processing::plan::SourceModel;
use crate::processing::station::Command;
use crate::processing::train::Train;
use crate::ui::ConfigModel;
use crate::util::Tx;
use crossbeam::channel::Sender;

pub trait Source: Send {
    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;

    fn add_out(&mut self, id: i64, out: Tx<Train>);

    fn get_stop(&self) -> i64;

    fn get_id(&self) -> i64;

    fn serialize(&self) -> SourceModel;

    fn from(stop_id: i64, configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
    where
        Self: Sized;

    fn serialize_default() -> Result<SourceModel, ()>
    where
        Self: Sized;
}

