use crate::algebra::{Algebra, Algebraic, BoxedIterator, ValueIterator};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::Layout;
use crate::util::storage::ValueStore;
use std::collections::HashMap;
use value::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Union {
    pub inputs: Vec<Algebraic>,
    distinct: bool,
}

impl InputDerivable for Union {
    fn derive_input_layout(&self) -> Option<Layout> {
        let input = self
            .inputs
            .iter()
            .map(|x| x.derive_input_layout())
            .collect::<Option<Vec<_>>>()?;
        Some(
            input
                .into_iter()
                .fold(Layout::default(), |a, b| a.merge(&b)),
        )
    }
}

impl OutputDerivable for Union {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        self.inputs.first().unwrap().derive_output_layout(inputs)
    }
}

impl Algebra for Union {
    type Iterator = UnionIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        let inputs: Vec<_> = self
            .inputs
            .iter_mut()
            .by_ref()
            .map(|i| i.derive_iterator())
            .collect();
        if !inputs.is_empty() {
            UnionIterator {
                inputs,
                distinct: self.distinct,
                index: 0,
            }
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
