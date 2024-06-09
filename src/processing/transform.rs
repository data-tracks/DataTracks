use crate::processing::train::Train;

pub struct Transform {
    func: fn(Train) -> Train,
}

impl Transform {
    pub(crate) fn default() -> Self {
        Transform { func: |f| f }
    }
    pub(crate) fn new(func: fn(Train) -> Train) -> Self {
        Transform { func }
    }

    pub fn apply(&self, train: Train) -> Train {
        self.func(train)
    }
}