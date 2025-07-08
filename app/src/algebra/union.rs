use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::{Algebra, BoxedIterator, ValueIterator};
use crate::processing::transform::Transform;
use crate::processing::Layout;
use crate::util::storage::ValueStore;
use std::collections::HashMap;
use value::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Union {
    id: usize,
    distinct: bool,
}

impl AlgInputDerivable for Union {
    fn derive_input_layout(&self, root: &AlgebraRoot) -> Option<Layout> {
        let input = root
            .get_children(self.id)
            .iter()
            .map(|x| x.derive_input_layout(root))
            .collect::<Option<Vec<_>>>()?;
        Some(
            input
                .into_iter()
                .fold(Layout::default(), |a, b| a.merge(&b)),
        )
    }
}

impl AlgOutputDerivable for Union {
    fn derive_output_layout(
        &self,
        inputs: HashMap<String, Layout>,
        root: &AlgebraRoot,
    ) -> Option<Layout> {
        root.get_children(self.id)
            .first()
            .unwrap()
            .derive_output_layout(inputs, root)
    }
}

impl Algebra for Union {
    type Iterator = UnionIterator;

    fn id(&self) -> usize {
        self.id
    }

    fn replace_id(self, id: usize) -> Self {
        Self { id, ..self }
    }

    fn derive_iterator(&self, root: &AlgebraRoot) -> Result<Self::Iterator, String> {
        let inputs: Vec<_> = root
            .get_children(self.id)
            .iter()
            .by_ref()
            .map(|i| i.derive_iterator(root).unwrap())
            .collect();
        if !inputs.is_empty() {
            Ok(UnionIterator {
                inputs,
                distinct: self.distinct,
                index: 0,
            })
        } else {
            panic!("Cannot derive empty union iterator");
        }
    }
}

pub struct UnionIterator {
    distinct: bool,
    inputs: Vec<BoxedIterator>,
    index: usize,
}

impl Iterator for UnionIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(value) = self.inputs.get_mut(self.index)?.next() {
                return Some(value);
            } else if self.index < self.inputs.len() - 1 {
                self.index += 1;
            } else {
                return None;
            }
        }
    }
}

impl ValueIterator for UnionIterator {
    fn get_storages(&self) -> Vec<ValueStore> {
        self.inputs
            .iter()
            .map(|x| x.get_storages())
            .reduce(|mut a, mut b| {
                a.append(&mut b);
                a
            })
            .unwrap()
    }

    fn clone(&self) -> BoxedIterator {
        let mut inputs: Vec<BoxedIterator> = vec![];
        for iter in &self.inputs {
            inputs.push((*iter).clone());
        }
        Box::new(UnionIterator {
            distinct: self.distinct,
            inputs,
            index: 0,
        })
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        self.inputs = self
            .inputs
            .iter_mut()
            .map(|i| {
                let input = i.enrich(transforms.clone());
                if let Some(input) = input {
                    input
                } else {
                    (*i).clone()
                }
            })
            .collect();
        None
    }
}
