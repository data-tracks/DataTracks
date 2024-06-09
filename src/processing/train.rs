use crate::value::Value;

#[derive(Clone)]
pub struct Train {
    pub course: Vec<i64>,
    pub values: Vec<Value>,
}

impl Train {
    pub(crate) fn new(values: Vec<Value>) -> Self {
        Train { course: vec![], values }
    }

    pub(crate) fn empty() -> Self {
        Train { course: vec![], values: vec![] }
    }
}