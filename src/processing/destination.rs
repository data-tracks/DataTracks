use std::sync::mpsc;

use crate::processing::train::Train;

pub trait Destination: Send {
    fn operate(&mut self);
    fn get_in(&self) -> mpsc::Sender<Train>;

    fn get_stop(&self) -> i64;
}