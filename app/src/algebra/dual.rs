use crate::algebra::{Algebra, BoxedIterator, ValueIterator};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::{Layout, OutputType};
use value::Value;
use std::collections::HashMap;
use crate::util::storage::ValueStore;

// "Dummy" table to query for constants, one row
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Dual {}


impl Dual {
    pub fn new() -> Self {
        Dual {}
    }
}

impl Default for Dual {
    fn default() -> Self {
        Self::new()
    }
}

impl InputDerivable for Dual {
    fn derive_input_layout(&self) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl OutputDerivable for Dual {
    fn derive_output_layout(&self, _inputs: HashMap<String, &Layout>) -> Option<Layout> {
        Some(Layout::from(OutputType::Integer))
    }
}


impl Algebra for Dual {
    type Iterator = DualIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        DualIterator::new()
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

impl Default for DualIterator {
    fn default() -> Self {
        Self::new()
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
    fn set_storage(&mut self, storage: ValueStore) {
        // nothing on purpose
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(DualIterator::new())
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}
