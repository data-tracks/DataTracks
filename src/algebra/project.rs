use std::sync::Arc;

use crate::algebra::algebra::{Algebra, Handler, RefHandler};
use crate::algebra::AlgebraType;
use crate::processing::{Train, Referencer};
use crate::value::Value;

pub trait Project: Algebra {
    fn get_input(&self) -> &AlgebraType;
}

pub struct TrainProject {
    input: Box<AlgebraType>,
    project: Option<fn(Value) -> Value>,
}

struct ProjectHandler{
    input: Box<dyn RefHandler>,
    project: fn(Value) -> Value
    
}

impl RefHandler for ProjectHandler {
    fn process(&self, train: &mut Train) -> Train {
        let mut train = self.input.process(train);
        let projected = train.values.get_mut(&0).unwrap().take().unwrap().into_iter().map(|value: Value| (self.project)(value)).collect();
        Train::default(projected)
    }
}

impl Algebra for TrainProject {
    fn get_handler(&mut self) -> Box<dyn RefHandler> {
        let project = self.project.take().unwrap();
        let input = self.input.get_handler();
        Box::new(ProjectHandler{input, project})
    }
}

impl Project for TrainProject {
    fn get_input(&self) -> &AlgebraType {
        &self.input
    }
}