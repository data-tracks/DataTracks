use crate::algebra::{Algebra, AlgebraType, BoxedIterator, ValueIterator};
use crate::analyse::Layoutable;
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
use crate::value::Value;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Union {
    inputs: Vec<AlgebraType>,
    distinct: bool,
}

impl Union {
    pub fn new(inputs: Vec<AlgebraType>) -> Self {
        Self { inputs, distinct: true }
    }
}

impl Layoutable for Union {
    fn derive_input_layout(&self) -> Layout {
        self.inputs.iter().map(|x| x.derive_input_layout()).fold(Layout::default(), |a, b| a.merge(&b).unwrap())
    }

    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Layout {
        self.inputs.get(0).unwrap().derive_output_layout(inputs)
    }
}

impl Algebra for Union {
    type Iterator = UnionIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        let inputs: Vec<_> = self.inputs.iter_mut().by_ref().map(|i| i.derive_iterator()).collect();
        if !inputs.is_empty() {
            UnionIterator{ inputs , distinct: self.distinct, index: 0 }
        }else {
            panic!("Cannot derive empty union iterator");
        }
    }

}

pub struct UnionIterator {
    distinct: bool,
    inputs: Vec<BoxedIterator>,
    index: usize
}

impl Iterator for UnionIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(value) = self.inputs.get_mut(self.index)?.next(){
                return Some(value);
            }else if self.index < self.inputs.len() - 1 {
                self.index += 1;
            }else {
                return None;
            }
        }
    }
}

impl ValueIterator for UnionIterator {
    fn load(&mut self, trains: Vec<Train>) {
        for input in &mut self.inputs {
            input.load(trains.clone());
        }
    }

    fn clone(&self) -> BoxedIterator {
        let mut inputs:Vec<BoxedIterator> = vec![];
        for iter in &self.inputs {
            inputs.push((*iter).clone());
        }
        Box::new(UnionIterator{distinct: self.distinct, inputs, index: 0 })
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        self.inputs = self.inputs.iter_mut().map(|i| {
            let input = i.enrich(transforms.clone());
            if let Some(input) = input {
                input
            } else {
                (*i).clone()
            }
        }).collect();
        None
    }
}