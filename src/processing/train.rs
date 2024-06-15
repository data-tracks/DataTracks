use std::collections::HashMap;

use crate::value::Value;

#[derive(Clone)]
pub struct Train {
    pub last: i64,
    pub values: HashMap<i64, Vec<Value>>,
}

impl Train {
    pub(crate) fn single(values: Vec<Value>) -> Self {
        let mut map = HashMap::new();
        map.insert(0, values);
        Train::new(map)
    }

    pub(crate) fn new(values: HashMap<i64, Vec<Value>>) -> Self {
        Train { last: -1, values }
    }


    pub(crate) fn visit(&mut self, stop: i64) {
        self.last = stop;
    }
}