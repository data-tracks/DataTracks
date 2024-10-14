use crate::algebra::{BoxedIterator, ValueIterator};
use crate::processing::{Plan, Train};
use crate::value::Value;

#[derive(Debug, Clone)]
pub struct VariableIterator {

}

impl Iterator for VariableIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl ValueIterator for VariableIterator {
    fn enrich(&mut self, plan: &Plan) {
        panic!()
    }


    fn load(&mut self, _trains: Vec<Train>) {
        // empty on purpose
    }

    fn clone(&self) -> BoxedIterator {
        todo!()
    }
}