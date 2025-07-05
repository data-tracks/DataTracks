use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::{Algebra, BoxedIterator, BoxedValueHandler, ValueIterator};
use crate::processing::transform::Transform;
use crate::processing::{Direction, Layout, Order};
use crate::util::storage::ValueStore;
use std::collections::{BTreeMap, HashMap};
use value::Value;

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
        Some(
            root.get_child(self.id)?
                .derive_output_layout(inputs, root)?,
        )
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
    input: BoxedIterator,
    handler: BoxedValueHandler,
    sorted: BTreeMap<Value, Vec<Value>>,
}

impl Iterator for SortIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        let handler = self.handler.clone();
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
    fn get_storages(&self) -> Vec<ValueStore> {
        self.input.get_storages()
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(SortIterator {
            direction: self.direction.clone(),
            input: self.input.clone(),
            handler: self.handler.clone(),
            sorted: self.sorted.clone(),
        })
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        self.input.enrich(transforms)
    }
}
