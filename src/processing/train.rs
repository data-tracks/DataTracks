use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use crate::value::Value;

pub type MutWagonsFunc = Box<dyn FnMut(&mut Vec<Train>)>;

#[derive(Clone, Debug, Deserialize, Serialize, Writable, Readable)]
pub struct Train {
    pub last: usize,
    pub values: Option<Vec<Value>>,
}

impl Train {
    pub fn new(stop: usize, values: Vec<Value>) -> Self {
        Train { last: stop, values: Some(values) }
    }


    pub(crate) fn set_last(&mut self, stop: usize) {
        self.last = stop;
    }
}

impl From<&mut Train> for Train {
    fn from(train: &mut Train) -> Self {
        Train::new(train.last, train.values.take().unwrap())
    }
}

impl From<Vec<Train>> for Train {
    fn from(wagons: Vec<Train>) -> Self {
        if wagons.len() == 1 {
            return wagons[0].clone()
        }

        let mut values = vec![];
        for mut train in wagons {
            values.append(train.values.take().unwrap().as_mut());
        }

        Train::new(0, values)
    }
}
