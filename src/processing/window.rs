use crate::processing::train::Train;

pub struct Window {
    func: fn(Train) -> Train,
}

impl Window {
    pub fn new(func: fn(Train) -> Train) -> Self {
        Window { func }
    }

    pub(crate) fn default() -> Self {
        Window { func: |t| t }
    }

    pub(crate) fn apply(&self, train: Train) -> Train {
        (self.func)(train)
    }
}