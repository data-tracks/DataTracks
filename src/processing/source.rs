use std::sync::mpsc;

use crate::processing::train::Train;

pub trait Source: Send {
    fn operate(&mut self);

    fn add_out(&mut self, id: i64, out: mpsc::Sender<Train>);

    fn get_stop(&self) -> i64;
}

