use crate::algebra::algebra::{Algebra, ValueIterator};
use crate::algebra::BoxedIterator;
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::Layout;
use crate::util::storage::ValueStore;
use crate::util::EmptyIterator;
use std::collections::{HashMap, VecDeque};
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
    storage: ValueStore,
}

impl ScanIterator {
    pub(crate) fn load(&mut self) -> bool {
        self.values = VecDeque::from(self.storage.drain());
        self.values.is_empty()
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
    fn get_storages(&self) -> Vec<ValueStore> {
        vec![self.storage.clone()]
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(ScanIterator {
            index: self.index,
            values: VecDeque::new(),
            storage: ValueStore::new(),
        })
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
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
        let storage = ValueStore::new_with_id(self.index);
        ScanIterator {
            index: self.index,
            values: VecDeque::new(),
            storage,
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
    use value::Value;

    #[test]
    fn simple_scan() {
        let train = Train::new(transform(vec![3.into(), "test".into()]));

        let mut scan = IndexScan::new(0);

        let mut handler = scan.derive_iterator();
        let binding = handler.get_storages();
        let storage = binding.first().unwrap();

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
