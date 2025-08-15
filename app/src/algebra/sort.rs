use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::{Algebra};
use crate::processing::{Direction, Layout, Order};
use core::util::reservoir::ValueReservoir;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use value::Value;
use core::{BoxedValueHandler, ValueIterator, BoxedValueIterator};


#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Sort {
    id: usize,
    pub order: Order,
}

impl AlgInputDerivable for Sort {
    fn derive_input_layout(&self, root: &AlgebraRoot) -> Option<Layout> {
        root.get_child(self.id).unwrap().derive_input_layout(root)
    }
}

impl AlgOutputDerivable for Sort {
    fn derive_output_layout(
        &self,
        inputs: HashMap<String, Layout>,
        root: &AlgebraRoot,
    ) -> Option<Layout> {
        root.get_child(self.id)?.derive_output_layout(inputs, root)
    }
}

impl Algebra for Sort {
    type Iterator = SortIterator;

    fn id(&self) -> usize {
        self.id
    }

    fn replace_id(self, id: usize) -> Self {
        Self { id, ..self }
    }

    fn derive_iterator(&self, root: &AlgebraRoot) -> Result<Self::Iterator, String> {
        let res = self.order.derive_handler();

        res.map(|(handler, dir)| SortIterator {
            direction: dir,
            input: root
                .get_child(self.id)
                .ok_or("No child in Sort.")
                .unwrap()
                .derive_iterator(root)
                .unwrap(),
            handler,
            sorted: BTreeMap::new(),
        })
        .ok_or(String::from("Could not derive iterator"))
    }
}

pub struct SortIterator {
    direction: Direction,
    input: BoxedValueIterator,
    handler: BoxedValueHandler,
    sorted: BTreeMap<Value, Vec<Value>>,
}

impl Iterator for SortIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        let handler = self.handler.clone_boxed();
        for val in self.input.drain() {
            self.sorted
                .entry(handler.process(&val))
                .or_default()
                .push(val);
        }

        let values = if self.direction == Direction::Asc {
            self.sorted.pop_first()
        } else {
            self.sorted.pop_last()
        };

        match values {
            None => None,
            Some((_, mut v)) => {
                let value = v.pop();
                if !v.is_empty() {
                    v.iter()
                        .for_each(|v| self.sorted.entry(v.clone()).or_default().push(v.clone()));
                };
                value
            }
        }
    }
}

impl ValueIterator for SortIterator {
    fn get_storages(&self) -> Vec<ValueReservoir> {
        self.input.get_storages()
    }

    fn clone_boxed(&self) -> BoxedValueIterator {
        Box::new(SortIterator {
            direction: self.direction.clone(),
            input: self.input.clone_boxed(),
            handler: self.handler.clone_boxed(),
            sorted: self.sorted.clone(),
        })
    }

    fn enrich(&mut self, transforms: Rc<HashMap<String, BoxedValueIterator>>) -> Option<BoxedValueIterator> {
        self.input.enrich(transforms)
    }
}
