use crate::algebra::{Algebra, Algebraic, BoxedIterator, BoxedValueHandler, ValueIterator};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::{Direction, Layout, Order};
use crate::util::storage::ValueStore;
use std::collections::{BTreeMap, HashMap};
use value::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Sort {
    pub input: Box<Algebraic>,
    pub order: Order,
}

impl InputDerivable for Sort {
    fn derive_input_layout(&self) -> Option<Layout> {
        self.input.derive_input_layout()
    }
}

impl OutputDerivable for Sort {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        let child_output = self.input.derive_output_layout(inputs);
        child_output.map(|mut layout| {
            layout.order = self.order.clone();
            layout
        })
    }
}

impl Algebra for Sort {
    type Iterator = SortIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        let res = self.order.derive_handler();

        if let Some((handler, dir)) = res {
            SortIterator {
                direction: dir,
                input: self.input.derive_iterator(),
                handler,
                sorted: BTreeMap::new(),
            }
        } else {
            panic!("Shouldn't happen");
        }
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

        let values = if self.direction == Direction::ASC {
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
    fn set_storage(&mut self, storage: ValueStore) {
        self.input.set_storage(storage);
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
