use crate::algebra::algebra::{Algebra, RefHandler, ValueIterator};
use crate::algebra::BoxedIterator;
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
use crate::util::storage::ValueStore;
use crate::util::EmptyIterator;
use std::collections::{HashMap, VecDeque};
use std::vec;
use value::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct IndexScan {
    index: usize,
}

impl IndexScan {
    pub(crate) fn new(index: usize) -> Self {
        IndexScan { index }
    }
}

#[derive(Clone)]
pub struct ScanIterator {
    index: usize,
    values: VecDeque<Value>,
    storage: Option<ValueStore>,
}

impl ScanIterator {
    pub(crate) fn load(&mut self) -> bool {
        match &self.storage {
            None => true,
            Some(store) => {
                self.values = VecDeque::from(store.drain());
                self.values.is_empty()
            }
        }
    }
}

impl Iterator for ScanIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() && self.load() {
            return None;
        }
        Some(self.values.pop_front().unwrap().wagonize(self.index))
    }
}

impl ValueIterator for ScanIterator {
    fn set_storage(&mut self, storage: ValueStore) {
        if storage.index == self.index {
            self.storage = Some(storage);
        }
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(ScanIterator {
            index: self.index,
            values: VecDeque::new(),
            storage: None,
        })
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}

impl RefHandler for ScanIterator {
    fn process(&self, _stop: usize, wagons: Vec<Train>) -> Vec<Train> {
        let mut values = vec![];
        wagons
            .into_iter()
            .filter(|w| w.last() == self.index)
            .for_each(|mut t| values.append(t.values.as_mut()));
        vec![Train::new(values).mark(self.index)]
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        Box::new(ScanIterator {
            index: self.index,
            values: VecDeque::new(),
            storage: self.storage.clone(),
        })
    }
}

impl InputDerivable for IndexScan {
    fn derive_input_layout(&self) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl OutputDerivable for IndexScan {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        inputs
            .get(self.index.to_string().as_str())
            .cloned()
            .cloned()
    }
}

impl Algebra for IndexScan {
    type Iterator = ScanIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        ScanIterator {
            index: self.index,
            values: VecDeque::new(),
            storage: None,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Scan {
    name: String,
}

impl Scan {
    pub fn new(name: String) -> Self {
        Scan { name }
    }
}

impl Clone for Scan {
    fn clone(&self) -> Self {
        Scan {
            name: self.name.clone(),
        }
    }
}

impl InputDerivable for Scan {
    fn derive_input_layout(&self) -> Option<Layout> {
        None
    }
}

impl OutputDerivable for Scan {
    fn derive_output_layout(&self, _inputs: HashMap<String, &Layout>) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl Algebra for Scan {
    type Iterator = EmptyIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        EmptyIterator {}
    }
}

#[cfg(test)]
mod test {
    use crate::algebra::algebra::Algebra;
    use crate::algebra::scan::IndexScan;
    use crate::algebra::ValueIterator;
    use crate::processing::Train;
    use crate::util::storage::ValueStore;
    use value::Value;

    #[test]
    fn simple_scan() {
        let train = Train::new(transform(vec![3.into(), "test".into()]));

        let mut scan = IndexScan::new(0);

        let storage = ValueStore::new();
        let mut handler = scan.derive_iterator();

        storage.append(train.values);

        let train_2 = handler.drain_to_train(0);

        assert_eq!(train_2.values, transform(vec![3.into(), "test".into()]));
        assert_ne!(train_2.values, transform(vec![8.into(), "test".into()]));
    }

    pub fn transform(values: Vec<Value>) -> Vec<Value> {
        let mut dicts = vec![];
        for value in values {
            dicts.push(Value::Dict(value.into()));
        }
        dicts
    }
}
