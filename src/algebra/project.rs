use crate::algebra::algebra::{Algebra, RefHandler};
use crate::algebra::AlgebraType;
use crate::processing::Train;
use crate::value::Value;

pub trait Project: Algebra {
    fn get_input(&self) -> &AlgebraType;
}

pub struct TrainProject {
    input: Box<AlgebraType>,
    project: Option<fn(Value) -> Value>,
}

struct ProjectHandler{
    input: Box<dyn RefHandler + Send>,
    project: fn(Value) -> Value
    
}

impl RefHandler for ProjectHandler {
    fn process(&self, stop: i64, wagons: &mut Vec<Train>) -> Train {
        let mut train = self.input.process(stop, wagons);
        let projected = train.values.take().unwrap().into_iter().map(|value: Value| (self.project)(value)).collect();
        Train::new(stop, projected)
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        RefHandler::clone(self)
    }
}

impl Algebra for TrainProject {
    fn get_handler(&mut self) -> Box<dyn RefHandler + Send> {
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