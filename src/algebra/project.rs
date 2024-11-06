use crate::algebra::algebra::{Algebra, ValueHandler};
use crate::algebra::function::Operator;
use crate::algebra::implement::implement;
use crate::algebra::{AlgebraType, BoxedIterator, ValueIterator};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
use crate::value::Value;
use std::collections::HashMap;

#[derive(Clone, Debug)]
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
            return Some(self.project.process(&value))
        }
        None
    }
}

impl ValueIterator for ProjectIterator {
    fn dynamically_load(&mut self, trains: Vec<Train>) {
        self.input.dynamically_load(trains);
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(ProjectIterator {input: self.input.clone(), project: self.project.clone()})
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        let input = self.input.enrich(transforms);

        if let Some(input) = input {
            self.input = input;
        };
        None
    }
}

impl InputDerivable for Project {
    fn derive_input_layout(&self) -> Layout {
        self.project.derive_input_layout()
    }
}

impl OutputDerivable for Project {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Layout {
        self.project.derive_output_layout(inputs)
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