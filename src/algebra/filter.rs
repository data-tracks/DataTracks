use crate::algebra::algebra::{Algebra, BoxedIterator, ValueIterator};
use crate::algebra::implement::implement;
use crate::algebra::{AlgebraType, BoxedValueHandler, Operator};
use crate::processing::Train;
use crate::value::Value;


#[derive(Clone)]
pub struct Filter {
    input: Box<AlgebraType>,
    condition: Operator,
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
            if let Ok(bool) = self.condition.process(value.clone()).as_bool(){
                if bool.0 {
                    return Some(value)
                }
            }
        }
        None
    }
}

impl ValueIterator for FilterIterator {
    fn load(&mut self, trains: Vec<Train>) {
        self.input.load(trains);
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(FilterIterator {input: self.input.clone(), condition: self.condition.clone()})
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