use crate::algebra::algebra::{Algebra, BoxedIterator, ValueIterator};
use crate::algebra::implement::implement;
use crate::algebra::{AlgebraType, BoxedValueHandler, Operator};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
use value::Value;
use std::collections::HashMap;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Filter {
    pub input: Box<AlgebraType>,
    pub condition: Operator,
}

impl Filter {
    pub fn new(input: AlgebraType, condition: Operator) -> Self {
        Filter { input: Box::new(input), condition }
    }
}


pub struct FilterIterator {
    input: BoxedIterator,
    condition: BoxedValueHandler
}

impl Iterator for FilterIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        for value in self.input.by_ref() {
            if let Ok(bool) = self.condition.process(&value).as_bool() {
                if bool.0 {
                    return Some(value)
                }
            }
        }
        None
    }
}

impl ValueIterator for FilterIterator {
    fn dynamically_load(&mut self, trains: Vec<Train>) {
        self.input.dynamically_load(trains);
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(FilterIterator {input: self.input.clone(), condition: self.condition.clone()})
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        let input = self.input.enrich(transforms);

        if let Some(input) = input {
            self.input = Box::new(FilterIterator { input, condition: self.condition.clone() });
        };
        None
    }
}

impl InputDerivable for Filter {
    fn derive_input_layout(&self) -> Option<Layout> {
        self.input.derive_input_layout().map(|l| l.merge(&self.condition.derive_input_layout().unwrap_or_default()))
    }
}

impl OutputDerivable for Filter {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        self.input.derive_output_layout(inputs)
    }
}

impl Algebra for Filter {
    type Iterator = FilterIterator;

    fn derive_iterator(&mut self) -> FilterIterator {
        let condition = implement(&self.condition);
        let input = self.input.derive_iterator();
        FilterIterator { input, condition }
    }

}