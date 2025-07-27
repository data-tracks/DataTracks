use crate::algebra::algebra::{Algebra, BoxedIterator, ValueIterator};
use crate::algebra::implement::implement;
use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::{BoxedValueHandler, Operator};
use crate::analyse::InputDerivable;
use crate::processing::transform::Transform;
use crate::processing::Layout;
use crate::util::reservoir::ValueReservoir;
use std::collections::HashMap;
use value::Value;

/// Applies filter operations like "WHERE name = 'Peter'"
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Filter {
    id: usize,
    pub condition: Operator,
}

impl Filter {
    pub fn new(id: usize, condition: Operator) -> Self {
        Filter { id, condition }
    }
}

pub struct FilterIterator {
    input: BoxedIterator,
    condition: BoxedValueHandler,
}

impl Iterator for FilterIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        for value in self.input.by_ref() {
            if let Ok(bool) = self.condition.process(&value).as_bool() {
                if bool.0 {
                    return Some(value);
                }
            }
        }
        None
    }
}

impl ValueIterator for FilterIterator {
    fn get_storages(&self) -> Vec<ValueReservoir> {
        self.input.get_storages()
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(FilterIterator {
            input: self.input.clone(),
            condition: self.condition.clone(),
        })
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        let input = self.input.enrich(transforms);

        if let Some(input) = input {
            self.input = Box::new(FilterIterator {
                input,
                condition: self.condition.clone(),
            });
        };
        None
    }
}

impl AlgInputDerivable for Filter {
    fn derive_input_layout(&self, _root: &AlgebraRoot) -> Option<Layout> {
        self.condition.derive_input_layout()
    }
}

impl AlgOutputDerivable for Filter {
    fn derive_output_layout(
        &self,
        inputs: HashMap<String, Layout>,
        root: &AlgebraRoot,
    ) -> Option<Layout> {
        root.get_child(self.id())?
            .derive_output_layout(inputs, root)
    }
}

impl Algebra for Filter {
    type Iterator = FilterIterator;

    fn id(&self) -> usize {
        self.id
    }

    fn replace_id(self, id: usize) -> Self {
        Self { id, ..self }
    }

    fn derive_iterator(&self, root: &AlgebraRoot) -> Result<Self::Iterator, String> {
        let condition = implement(&self.condition);
        let input = root
            .get_child(self.id())
            .ok_or("No child in Filter")?
            .derive_iterator(root)?;
        Ok(FilterIterator { input, condition })
    }
}
