use crate::algebra::algebra::{Algebra, IdentityHandler, ValueHandler};
use crate::algebra::function::Operator;
use crate::algebra::implement::implement;
use crate::algebra::operator::SetProjectIterator;
use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::{BoxedIterator, Op, ValueIterator};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::Layout;
use crate::util::storage::ValueStore;
use std::collections::HashMap;
use value::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Project {
    id: usize,
    pub project: Operator,
}

impl Project {
    pub fn new(id: usize, project: Operator) -> Self {
        Project { id, project }
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
        match self {
            ProjectIter::ValueProjectIterator(p) => p.get_storages(),
            ProjectIter::ValueSetProjectIterator(s) => s.get_storages(),
        }
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

impl AlgInputDerivable for Project {
    fn derive_input_layout(&self, _root: &AlgebraRoot) -> Option<Layout> {
        self.project.derive_input_layout()
    }
}

impl AlgOutputDerivable for Project {
    fn derive_output_layout(
        &self,
        inputs: HashMap<String, Layout>,
        _root: &AlgebraRoot,
    ) -> Option<Layout> {
        self.project.derive_output_layout(inputs)
    }
}

impl Algebra for Project {
    type Iterator = ProjectIter;

    fn id(&self) -> usize {
        self.id
    }

    fn replace_id(self, id: usize) -> Self {
        Self {
            id,
            ..self
        }
    }

    fn derive_iterator(&self, root: &AlgebraRoot) -> Result<Self::Iterator, String> {
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
            Ok(ProjectIter::ValueSetProjectIterator(
                SetProjectIterator::new(
                    root.get_child(self.id)
                        .ok_or("No child in Project.")?
                        .derive_iterator(root)?,
                    op,
                ),
            ))
        } else {
            let project = implement(&self.project);
            let input = root
                .get_child(self.id)
                .ok_or("No child in Project.")?
                .derive_iterator(root)?;
            Ok(ProjectIter::ValueProjectIterator(ProjectIterator {
                input,
                project,
            }))
        }
    }
}
