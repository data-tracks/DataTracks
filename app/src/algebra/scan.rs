use crate::algebra::algebra::{Algebra, ValueIterator};
use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::BoxedIterator;
use crate::processing::transform::Transform;
use crate::processing::Layout;
use crate::util::reservoir::ValueReservoir;
use crate::util::EmptyIterator;
use std::collections::{HashMap, VecDeque};
use value::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct IndexScan {
    id: usize,
    scan_index: usize,
}

impl IndexScan {
    pub(crate) fn new(id: usize, scan_index: usize) -> Self {
        Self { id, scan_index }
    }
}

#[derive(Clone)]
pub struct ScanIterator {
    index: usize,
    values: VecDeque<Value>,
    storage: ValueReservoir,
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
    fn get_storages(&self) -> Vec<ValueReservoir> {
        vec![self.storage.clone()]
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(ScanIterator {
            index: self.index,
            values: VecDeque::new(),
            storage: ValueReservoir::new(),
        })
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}

impl AlgInputDerivable for IndexScan {
    fn derive_input_layout(&self, _root: &AlgebraRoot) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl AlgOutputDerivable for IndexScan {
    fn derive_output_layout(
        &self,
        inputs: HashMap<String, Layout>,
        _root: &AlgebraRoot,
    ) -> Option<Layout> {
        inputs.get(self.scan_index.to_string().as_str()).cloned()
    }
}

impl Algebra for IndexScan {
    type Iterator = ScanIterator;

    fn id(&self) -> usize {
        self.id
    }

    fn replace_id(self, id: usize) -> Self {
        Self { id, ..self }
    }

    fn derive_iterator(&self, _root: &AlgebraRoot) -> Result<Self::Iterator, String> {
        let storage = ValueReservoir::new_with_id(self.scan_index);
        Ok(ScanIterator {
            index: self.scan_index,
            values: VecDeque::new(),
            storage,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Scan {
    id: usize,
    name: String,
}

impl Scan {
    pub fn new(name: String, id: usize) -> Self {
        Scan { name, id }
    }
}

impl Clone for Scan {
    fn clone(&self) -> Self {
        Scan {
            name: self.name.clone(),
            id: self.id,
        }
    }
}

impl AlgInputDerivable for Scan {
    fn derive_input_layout(&self, _root: &AlgebraRoot) -> Option<Layout> {
        None
    }
}

impl AlgOutputDerivable for Scan {
    fn derive_output_layout(
        &self,
        _inputs: HashMap<String, Layout>,
        _root: &AlgebraRoot,
    ) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl Algebra for Scan {
    type Iterator = EmptyIterator;

    fn id(&self) -> usize {
        self.id
    }

    fn replace_id(self, id: usize) -> Self {
        Self { id, ..self }
    }

    fn derive_iterator(&self, _root: &AlgebraRoot) -> Result<Self::Iterator, String> {
        Ok(EmptyIterator {})
    }
}

#[cfg(test)]
mod test {
    use crate::algebra::AlgebraRoot;
    use crate::processing::Train;
    use value::Value;

    #[test]
    fn simple_scan() {
        let train = Train::new(transform(vec![3.into(), "test".into()]), 0);

        let mut root = AlgebraRoot::new_scan_index(0);

        let mut handler = root.derive_iterator().unwrap();
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
