use crate::algebra::algebra::{Algebra, RefHandler, ValueIterator};
use crate::algebra::BoxedIterator;
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
use crate::util::EmptyIterator;
use std::collections::HashMap;
use std::vec;
use value::Value;
use crate::util::storage::Storage;

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
pub struct ScanIterator<'a> {
    index: usize,
    values: Vec<Value>,
    trains: Vec<Train>,
    storage: Option<&'a Storage>
}

impl ScanIterator {
    pub(crate) fn next_train(&mut self) -> bool {
        loop {
            if self.trains.is_empty() {
                return false;
            } else {
                let mut train = self.trains.remove(0);
                if let Some(mut values) = train.values.take() {
                    if !values.is_empty() {
                        self.values.append(&mut values);
                        return true;
                    }
                }
            }
        }
    }
}

impl Iterator for ScanIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() && !self.next_train() {
            return None
        }
        Some(self.values.remove(0))
    }
}

impl ValueIterator for ScanIterator {

    fn set_storage(&mut self, storage: &Storage) {
        todo!()
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(ScanIterator { index: self.index, values: vec![], trains: self.trains.clone(), storage: None })
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}

impl RefHandler for ScanIterator {
    fn process(&self, _stop: usize, wagons: Vec<Train>) -> Vec<Train> {
        let mut values = vec![];
        wagons.into_iter().filter(|w| w.last() == self.index).for_each(|mut t| values.append(t.values.take().unwrap().as_mut()));
        vec![Train::new(values).mark(self.index)]
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        Box::new(ScanIterator { index: self.index, values: vec![], trains: vec![], storage: self.storage })
    }
}

impl InputDerivable for IndexScan {
    fn derive_input_layout(&self) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl OutputDerivable for IndexScan {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        inputs.get(self.index.to_string().as_str()).cloned().cloned()
    }
}

impl Algebra for IndexScan {
    type Iterator<'a> = ScanIterator<'a>;

    fn derive_iterator(&mut self) -> Self::Iterator {
        ScanIterator { index: self.index, values: vec![], trains: vec![], storage: None }
    }

}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct TableScan {
    name: String,
}

impl TableScan {
    pub fn new(name: String) -> Self {
        TableScan { name }
    }
}

impl Clone for TableScan {
    fn clone(&self) -> Self {
        TableScan { name: self.name.clone() }
    }
}

impl InputDerivable for TableScan {
    fn derive_input_layout(&self) -> Option<Layout> {
        None
    }
}

impl OutputDerivable for TableScan {
    fn derive_output_layout(&self, _inputs: HashMap<String, &Layout>) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl Algebra for TableScan {
    type Iterator = EmptyIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        EmptyIterator{}
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
        handler.dynamically_load(train);

        let mut train_2 = handler.drain_to_train(0);

        assert_eq!(train_2.values.clone().unwrap(), transform(vec![3.into(), "test".into()]));
        assert_ne!(train_2.values.take().unwrap(), transform(vec![8.into(), "test".into()]));
    }

    pub fn transform(values: Vec<Value>) -> Vec<Value> {
        let mut dicts = vec![];
        for value in values {
            dicts.push(Value::Dict(value.into()));
        }
        dicts
    }
}