use crate::algebra::algebra::{Algebra, RefHandler, ValueHandler};
use crate::algebra::function::Function;
use crate::algebra::implement::implement;
use crate::processing::Train;

pub trait Project: Algebra {
    fn get_input(&self) -> &Box<dyn Algebra>;
}

pub struct TrainProject {
    input: Box<dyn Algebra>,
    project: Function,
}

struct ProjectHandler {
    input: Box<dyn RefHandler + Send>,
    project: Box<dyn ValueHandler + Send>,
}

impl RefHandler for ProjectHandler {
    fn process(&self, stop: i64, wagons: Vec<Train>) -> Train {
        let mut train = self.input.process(stop, wagons);
        let projected = train.values.take().unwrap().into_iter().map(|value| self.project.process(value)).collect();
        Train::new(stop, projected)
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        Box::new(ProjectHandler { input: self.input.clone(), project: self.project.clone() })
    }
}

impl Algebra for TrainProject {
    fn get_handler(&mut self) -> Box<dyn RefHandler + Send> {
        let project = implement(&self.project);
        let input = self.input.get_handler();
        Box::new(ProjectHandler { input, project })
    }
}

impl Project for TrainProject {
    fn get_input(&self) -> &Box<dyn Algebra> {
        &self.input
    }
}