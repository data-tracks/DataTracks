use crate::algebra::{Algebra, BoxedIterator, ValueIterator};
use crate::processing::transform::Transform;
use crate::processing::{Layout, OutputType, Train};
use crate::value::Value;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Dual {}


impl Dual {
    pub fn new() -> Self {
        Dual {}
    }
}

impl Algebra for Dual {
    type Iterator = DualIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        todo!()
    }

    fn derive_input_layout(&self) -> Layout {
        Layout::default()
    }

    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Layout {
        Layout::new(OutputType::Integer)
    }
}

pub struct DualIterator {
    consumed: bool,
}

impl DualIterator {
    pub fn new() -> Self {
        Self { consumed: true }
    }
}

impl Iterator for DualIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.consumed {
            None
        }else {
            self.consumed = false;
            Some(Value::int(1))
        }
    }
}

impl ValueIterator for DualIterator {
    fn load(&mut self, _trains: Vec<Train>) {
        // nothing on purpose
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(DualIterator::new())
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}