use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use crate::value::{Time, Value};

pub type MutWagonsFunc = Box<dyn FnMut(&mut Vec<Train>)>;

#[derive(Clone, Debug, Deserialize, Serialize, Writable, Readable)]
pub struct Train {
    pub marks: HashMap<usize, Time>,
    pub values: Option<Vec<Value>>,
}

impl Train {
    pub fn new(values: Vec<Value>) -> Self {
        Train { marks: HashMap::new(), values: Some(values) }
    }
    
    pub fn mark(self, stop: usize) -> Self {
        self.mark_timed(stop, Time::now())
    }
    
    pub fn mark_timed(mut self, stop: usize, time: Time) -> Self {
        self.marks.insert(stop, time);
        self
    }
    
    pub fn last(&self) -> usize {
        self.marks.iter().last().map(|(key,_)| *key).unwrap_or_default()
    }
    
}

impl From<&mut Train> for Train {
    fn from(train: &mut Train) -> Self {
        let mut train = Train::new(train.values.take().unwrap());
        train.marks = train.marks.iter().map(|(k, v)| (*k, v.clone())).collect();
        train
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

        let train = Train::new( values);
        train
    }
}
