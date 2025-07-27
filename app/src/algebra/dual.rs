use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::{Algebra, BoxedIterator, ValueIterator};
use crate::processing::transform::Transform;
use crate::processing::{Layout, OutputType};
use crate::util::reservoir::ValueReservoir;
use std::collections::HashMap;
use value::Value;

/// "Dummy" table to query for constants, one row
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Dual {
    id: usize,
}

impl Dual {
    pub fn new(id: usize) -> Self {
        Dual { id }
    }
}

impl AlgInputDerivable for Dual {
    fn derive_input_layout(&self, _root: &AlgebraRoot) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl AlgOutputDerivable for Dual {
    fn derive_output_layout(
        &self,
        _inputs: HashMap<String, Layout>,
        _root: &AlgebraRoot,
    ) -> Option<Layout> {
        Some(Layout::from(OutputType::Integer))
    }
}

impl Algebra for Dual {
    type Iterator = DualIterator;

    fn id(&self) -> usize {
        self.id
    }

    fn replace_id(self, id: usize) -> Self {
        Self { id }
    }

    fn derive_iterator(&self, _root: &AlgebraRoot) -> Result<Self::Iterator, String> {
        Ok(DualIterator::new())
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
        } else {
            self.consumed = false;
            Some(Value::int(1))
        }
    }
}

impl ValueIterator for DualIterator {
    fn get_storages(&self) -> Vec<ValueReservoir> {
        vec![]
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(DualIterator::new())
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}
