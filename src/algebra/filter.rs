use crate::algebra::algebra::{Algebra, RefHandler, ValueHandler};
use crate::algebra::implement::implement;
use crate::algebra::{AlgebraType, Function};
use crate::processing::Train;

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


struct FilterHandler{
    input: Box<dyn RefHandler>,
    condition: Box<dyn ValueHandler>
}

impl RefHandler for  FilterHandler{
    fn process(&self, stop: i64, wagons: Vec<Train>) -> Train {
        let mut train = self.input.process(stop, wagons);
        let filtered = train.values.take().unwrap().into_iter().filter(|v| self.condition.process(v.clone()).as_bool().unwrap().0).collect();
        Train::new(stop, filtered)
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        Box::new(FilterHandler { input: self.input.clone(), condition: self.condition.clone() })
    }
}


impl Algebra for TrainFilter {
    fn get_handler(&mut self) -> Box<dyn RefHandler + Send> {
        let condition = implement(&self.condition);
        let input = self.input.get_handler();
        Box::new(FilterHandler { input, condition })
    }
}

impl Filter for TrainFilter {
    fn get_input(&self) -> &AlgebraType {
        &self.input
    }
}