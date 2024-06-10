use crate::value::Value;

#[derive(Clone)]
pub struct Train {
    pub last: i64,
    pub values: Vec<Value>,
}

impl Train {
    pub(crate) fn new(values: Vec<Value>) -> Self {
        Train { last: -1, values }
    }

    pub(crate) fn empty() -> Self {
        Train::new(vec![])
    }

    pub(crate) fn visit(&mut self, stop: i64) {
        self.last = stop;
    }
}