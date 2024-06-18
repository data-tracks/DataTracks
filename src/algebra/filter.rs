use std::collections::HashMap;
use std::sync::Arc;

use crate::algebra::algebra::{Algebra, RefHandler};
use crate::algebra::AlgebraType;
use crate::processing::{Train, Referencer};
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
    fn process(&self, train: &mut Train) -> Train{
        let mut train = self.input.process(train);
        let filtered = train.values.get_mut(&0).unwrap().take().unwrap().into_iter().filter(|v| self.condition(v)).collect();
        Train::default(filtered)
    }
}


impl Algebra for TrainFilter {
    fn get_handler(&mut self) -> Box<dyn RefHandler> {
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