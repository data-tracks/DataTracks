use crate::algebra::algebra::{Algebra, ValueEnumerator, ValueRefHandler};
use crate::algebra::implement::implement;
use crate::algebra::{AlgebraType, Function};
use crate::processing::Train;
use crate::value::Value;

pub trait Filter: Algebra {
    fn get_input(&self) -> &AlgebraType;
}


pub struct TrainFilter {
    input: Box<AlgebraType>,
    condition: Function,
}

impl TrainFilter {
    pub fn new(input: AlgebraType, condition: Function) -> Self {
        TrainFilter { input: Box::new(input), condition }
    }
}


struct FilterEnumerator {
    input: Box<dyn ValueEnumerator<Item=Value>>,
    condition: Box<dyn ValueRefHandler>
}

impl Iterator for FilterEnumerator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(value) = self.input.next() {
            if self.condition.process(&value){
                return Some(value);
            }
        }
        None
    }
}

impl ValueEnumerator for FilterEnumerator {
    fn load(&mut self, trains: Vec<Train>) {
        self.input.load(trains);
    }

    fn clone(&self) -> Box<dyn ValueEnumerator<Item=Value> + Send + 'static> {
        Box::new(FilterEnumerator{input: Box::new(self.input.clone()), condition: self.condition.clone()})
    }
}


impl Algebra for TrainFilter {
    fn get_enumerator(&mut self) -> Box<dyn ValueEnumerator<Item=Value> + Send> {
        let condition = implement(&self.condition);
        let input = self.input.get_enumerator();
        Box::new(FilterEnumerator { input, condition })
    }
}

impl Filter for TrainFilter {
    fn get_input(&self) -> &AlgebraType {
        &self.input
    }
}