use std::collections::HashMap;
use tokio::io::AsyncReadExt;

use crate::value::Value;

#[derive(Clone)]
pub struct Train {
    pub last: i64,
    pub values: HashMap<i64, Option<Vec<Value>>>,
}

impl Train {
    pub(crate) fn default(values: Vec<Value>) -> Self {
        Self::single(0, values)
    }

    pub(crate) fn single(stop: i64, values: Vec<Value>) -> Self {
        let mut map = HashMap::new();
        map.insert(stop, values);
        Train::new(map)
    }

    pub(crate) fn new(values: HashMap<i64, Vec<Value>>) -> Self {
        Train { last: -1, values: values.into_iter().map(|(stop, vec)| (stop, Some(vec))).collect() }
    }


    pub(crate) fn visit(&mut self, stop: i64) {
        self.last = stop;
    }
}

impl From<&mut Train> for Train {
    fn from(train: &mut Train) -> Self {
        let mut values = HashMap::new();
        for (stop, value) in &mut train.values {
            values.insert(stop.clone(), value.take().unwrap());
        }
        Train::new(values)
    }
}
