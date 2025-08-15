use crate::algebra::algebra::Algebra;
use crate::algebra::implement::implement;
use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::Operator;
use crate::analyse::InputDerivable;
use crate::processing::Layout;
use core::util::reservoir::ValueReservoir;
use core::{BoxedValueIterator, BoxedValueHandler, ValueIterator};
use std::collections::HashMap;
use std::rc::Rc;
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
    input: BoxedValueIterator,
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

    fn clone_boxed(&self) -> BoxedValueIterator {
        Box::new(FilterIterator {
            input: self.input.clone_boxed(),
            condition: self.condition.clone_boxed(),
        })
    }

    fn enrich(&mut self, transforms: Rc<HashMap<String, BoxedValueIterator>>) -> Option<BoxedValueIterator> {
        let input = self.input.enrich(transforms);

        if let Some(input) = input {
            self.input = Box::new(FilterIterator {
                input,
                condition: self.condition.clone_boxed(),
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
