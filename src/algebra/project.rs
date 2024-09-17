use crate::algebra::algebra::{Algebra, ValueHandler};
use crate::algebra::function::Function;
use crate::algebra::implement::implement;
use crate::algebra::{AlgebraType, ValueEnumerator};
use crate::processing::Train;
use crate::value::Value;

pub trait Project: Algebra {
    fn get_input(&self) -> &Box<AlgebraType>;
}

pub struct TrainProject {
    input: Box<AlgebraType>,
    project: Function,
}

impl TrainProject {
    pub fn new(input: AlgebraType, project: Function) -> Self {
        TrainProject { input: Box::new(input), project }
    }
}


struct ProjectHandler {
    input: Box<dyn ValueEnumerator<Item=Value> + Send>,
    project: Box<dyn ValueHandler + Send>,
}

impl Iterator for ProjectHandler {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(value) = self.input.next() {
            return Some(self.project.process(value))
        }
        None
    }
}

impl ValueEnumerator for ProjectHandler {
    fn load(&mut self, trains: Vec<Train>) {
        self.input.load(trains);
    }
}

impl Algebra for TrainProject {
    fn get_enumerator(&mut self) -> Box<dyn ValueEnumerator<Item=Value> + Send> {
        let project = implement(&self.project);
        let input = self.input.get_enumerator();
        Box::new(ProjectHandler { input, project })
    }
}

impl Project for TrainProject {
    fn get_input(&self) -> &Box<AlgebraType> {
        &self.input
    }
}