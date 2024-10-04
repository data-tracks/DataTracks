use crate::processing::Train;

pub trait Depot {

    // get all values
    fn pull(&mut self) -> Vec<Train>;
}