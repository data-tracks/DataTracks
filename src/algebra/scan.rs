use crate::algebra::algebra::{Algebra, RefHandler, ValueIterator};
use crate::algebra::BoxedIterator;
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
use crate::value::Value;
use std::collections::HashMap;
use std::vec;
use crate::util::EmptyIterator;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct IndexScan {
    index: i64,
}

impl IndexScan {
    pub(crate) fn new(index: i64) -> Self {
        IndexScan { index }
    }
}

#[derive(Clone)]
pub struct ScanIterator {
    index: i64,
    values: Vec<Value>,
    trains: Vec<Train>,
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
    fn dynamically_load(&mut self, trains: Vec<Train>) {
        for mut train in trains {
            if train.last == self.index {
                train.values = Some(train.values.unwrap().into_iter().map(|d| {
                    let value = match d {
                        Value::Wagon(w) => {
                            w.unwrap()
                        }
                        v => v
                    };

                    Value::wagon(value, self.index.to_string())

                }).collect());
                self.trains.push(train);
            }
        }
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(ScanIterator { index: self.index, values: vec![], trains: self.trains.clone() })
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}

impl RefHandler for ScanIterator {
    fn process(&self, _stop: i64, wagons: Vec<Train>) -> Vec<Train> {
        let mut values = vec![];
        wagons.into_iter().filter(|w| w.last == self.index).for_each(|mut t| values.append(t.values.take().unwrap().as_mut()));
        vec![Train::new(self.index, values)]
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        Box::new(ScanIterator { index: self.index, values: vec![], trains: vec![] })
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
    type Iterator = ScanIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        ScanIterator { index: self.index, values: vec![], trains: vec![] }
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
    use crate::value::Dict;

    #[test]
    fn simple_scan() {
        let train = Train::new(0, Dict::transform(vec![3.into(), "test".into()]));

        let mut scan = IndexScan::new(0);

        let mut handler = scan.derive_iterator();
        handler.dynamically_load(vec![train]);

        let mut train_2 = handler.drain_to_train(0);

        assert_eq!(train_2.values.clone().unwrap(), Dict::transform(vec![3.into(), "test".into()]));
        assert_ne!(train_2.values.take().unwrap(), Dict::transform(vec![8.into(), "test".into()]));
    }
}