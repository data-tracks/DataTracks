use crate::algebra::algebra::{Algebra, IdentityHandler, ValueHandler};
use crate::algebra::function::Operator;
use crate::algebra::implement::implement;
use crate::algebra::operator::SetProjectIterator;
use crate::algebra::{Algebraic, BoxedIterator, Op, ValueIterator};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::Layout;
use crate::util::storage::ValueStore;
use std::collections::HashMap;
use tracing::warn;
use value::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Project {
    pub input: Box<Algebraic>,
    pub project: Operator,
}

impl Project {
    pub fn new(project: Operator, input: Algebraic) -> Self {
        Project {
            input: Box::new(input),
            project,
        }
    }
}

pub enum ProjectIter {
    ValueProjectIterator(ProjectIterator),
    ValueSetProjectIterator(SetProjectIterator),
}

impl Iterator for ProjectIter {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ProjectIter::ValueProjectIterator(p) => p.next(),
            ProjectIter::ValueSetProjectIterator(p) => p.next(),
        }
    }
}

impl ValueIterator for ProjectIter {
    fn get_storages(&self) -> Vec<ValueStore> {
        warn!("should not reach");
        vec![]
    }

    fn clone(&self) -> BoxedIterator {
        match self {
            ProjectIter::ValueProjectIterator(p) => p.clone(),
            ProjectIter::ValueSetProjectIterator(p) => p.clone(),
        }
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        match self {
            ProjectIter::ValueProjectIterator(p) => p.enrich(transforms),
            ProjectIter::ValueSetProjectIterator(p) => p.enrich(transforms),
        }
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
            return Some(self.project.process(&value));
        }
        None
    }
}

impl<'a> ValueIterator for ProjectIterator {
    fn get_storages(&self) -> Vec<ValueStore> {
        self.input.get_storages()
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(ProjectIterator {
            input: self.input.clone(),
            project: self.project.clone(),
        })
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
    fn derive_input_layout(&self) -> Option<Layout> {
        self.project.derive_input_layout()
    }
}

impl OutputDerivable for Project {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        self.project.derive_output_layout(inputs)
    }
}

impl Algebra for Project {
    type Iterator = ProjectIter;

    fn derive_iterator(&mut self) -> ProjectIter {
        if let Op::Collection(_) = &self.project.op {
            let op = self
                .project
                .operands
                .iter()
                .map(|o| implement(o))
                .collect::<Vec<_>>()
                .first()
                .map(|o| (*o).clone())
                .unwrap_or(IdentityHandler::new());
            return ProjectIter::ValueSetProjectIterator(SetProjectIterator::new(
                self.input.derive_iterator(),
                op,
            ));
        }

        let project = implement(&self.project);
        let input = self.input.derive_iterator();
        ProjectIter::ValueProjectIterator(ProjectIterator { input, project })
    }
}
