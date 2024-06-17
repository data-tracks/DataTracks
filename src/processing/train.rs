use std::collections::HashMap;

use crate::value::Value;

#[derive(Clone)]
pub struct Train {
    pub last: i64,
    pub values: HashMap<i64, Option<Vec<Value>>>,
}

impl Train {
    pub(crate) fn single(stop:i64, values: Vec<Value>) -> Self {
        let mut map = HashMap::new();
        map.insert(stop, Some(values));
        Train::new(map)
    }

    pub(crate) fn new(values: HashMap<i64, Option<Vec<Value>>>) -> Self {
        Train { last: -1, values }
    }


    pub(crate) fn visit(&mut self, stop: i64) {
        self.last = stop;
    }
}