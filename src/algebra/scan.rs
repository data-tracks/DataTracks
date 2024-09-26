use crate::algebra::algebra::{Algebra, RefHandler, ValueIterator};
use crate::processing::Train;
use crate::value::Value;
use std::vec;
use crate::algebra::BoxedIterator;

#[derive(Clone)]
pub struct Scan {
    index: i64,
}

impl Scan {
    pub(crate) fn new(index: i64) -> Self {
        Scan { index }
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
    fn load(&mut self, trains: Vec<Train>) {
        for train in trains {
            if train.last == self.index {
                self.trains.push(train);
            }
        }
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(ScanIterator { index: self.index, values: vec![], trains: self.trains.clone() })
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

impl Algebra for Scan {
    type Iterator = ScanIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        ScanIterator { index: self.index, values: vec![], trains: vec![] }
    }
}

#[cfg(test)]
mod test {
    use crate::algebra::algebra::Algebra;
    use crate::algebra::scan::Scan;
    use crate::algebra::ValueIterator;
    use crate::processing::Train;
    use crate::value::Dict;

    #[test]
    fn simple_scan() {
        let train = Train::new(0, Dict::transform(vec![3.into(), "test".into()]));

        let mut scan = Scan::new(0);

        let mut handler = scan.derive_iterator();
        handler.load(vec![train]);

        let mut train_2 = handler.drain_to_train(0);

        assert_eq!(train_2.values.clone().unwrap(), Dict::transform(vec![3.into(), "test".into()]));
        assert_ne!(train_2.values.take().unwrap(), Dict::transform(vec![8.into(), "test".into()]));
    }
}