use crate::algebra::algebra::{Algebra, RefHandler};
use crate::algebra::AlgebraType;
use crate::processing::Train;
use crate::value::Value;

pub trait Filter: Algebra {
    fn get_input(&self) -> &AlgebraType;
}


pub struct TrainFilter {
    input: Box<AlgebraType>,
    condition: Option<fn(&Value) -> bool>,
}


struct FilterHandler{
    input: Box<dyn RefHandler>,
    condition: fn(&Value) -> bool
}

impl RefHandler for  FilterHandler{
    fn process(&self, stop: i64, wagons: Vec<Train>) -> Train {
        let mut train = self.input.process(stop, wagons);
        let filtered = train.values.take().unwrap().into_iter().filter(|v| (self.condition)(v)).collect();
        Train::new(stop, filtered)
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        Box::new(FilterHandler{ input: self.input.clone(), condition: self.condition })
    }
}


impl Algebra for TrainFilter {
    fn get_handler(&mut self) -> Box<dyn RefHandler + Send> {
        let condition = self.condition.take().unwrap();
        let input = self.input.get_handler();
        Box::new(FilterHandler{input, condition})
    }
}

impl Filter for TrainFilter {
    fn get_input(&self) -> &AlgebraType {
        &self.input
    }
}