use value::train::Train;

pub struct Portal {}

/// Thread-safe implementation for a train saver
impl Portal {
    pub fn new() -> Self {
        Portal {}
    }

    pub fn push(&self, train: Train) {}

    pub fn drain(&self) -> Vec<Train> {
        todo!()
    }
}

impl Clone for Portal {
    fn clone(&self) -> Self {
        Portal {}
    }
}
