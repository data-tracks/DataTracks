use crate::algebra::{BoxedIterator, ValueIterator};
use crate::processing::Train;
use crate::value::Value;

#[derive(Debug, Clone)]
pub struct VariableIterator {

}

impl Iterator<Item=Value> for VariableIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl ValueIterator for VariableIterator {
    fn load(&mut self, trains: Vec<Train>) {
        // empty on purpose
    }

    fn clone(&self) -> BoxedIterator {
        todo!()
    }
}