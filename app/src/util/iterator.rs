use std::collections::HashMap;
use crate::algebra::{BoxedIterator, ValueIterator};
use crate::processing::Train;
use crate::processing::transform::Transform;
use value::Value;

pub struct EmptyIterator {}

impl Iterator for EmptyIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl ValueIterator for EmptyIterator {
    fn dynamically_load(&mut self, _trains: Vec<Train>) {
        // nothing on purpose
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(EmptyIterator {})
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}