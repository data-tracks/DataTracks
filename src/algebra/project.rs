use crate::algebra::algebra::{Algebra, ValueHandler};
use crate::algebra::function::Operator;
use crate::algebra::implement::implement;
use crate::algebra::{AlgebraType, BoxedIterator, ValueIterator};
use crate::processing::Train;
use crate::value::Value;

#[derive(Clone)]
pub struct Project {
    input: Box<AlgebraType>,
    project: Operator,
}

impl Project {
    pub fn new(input: AlgebraType, project: Operator) -> Self {
        Project { input: Box::new(input), project }
    }
}


pub struct ProjectIterator {
    input: BoxedIterator,
    project: Box<dyn ValueHandler + Send>,
}

impl Iterator for ProjectIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(value) = self.input.next() {
            return Some(self.project.process(value))
        }
        None
    }
}

impl ValueIterator for ProjectIterator {
    fn load(&mut self, trains: Vec<Train>) {
        self.input.load(trains);
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(ProjectIterator {input: self.input.clone(), project: self.project.clone()})
    }
}

impl Algebra for Project {
    type Iterator = ProjectIterator;

    fn derive_iterator(&mut self) -> ProjectIterator {
        let project = implement(&self.project);
        let input = self.input.derive_iterator();
        ProjectIterator { input, project }
    }
}