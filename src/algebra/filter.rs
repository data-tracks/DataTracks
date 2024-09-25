use crate::algebra::algebra::{Algebra, ValueEnumerator, ValueHandler};
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
    input: Box<dyn ValueEnumerator<Item=Value> + Send + 'static>,
    condition: Box<dyn ValueHandler>
}

impl Iterator for FilterEnumerator {
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

impl ValueEnumerator for FilterEnumerator {
    fn load(&mut self, trains: Vec<Train>) {
        self.input.load(trains);
    }

    fn clone(&self) -> Box<dyn ValueEnumerator<Item=Value> + Send + 'static> {
        Box::new(FilterEnumerator{input: self.input.clone(), condition: self.condition.clone()})
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