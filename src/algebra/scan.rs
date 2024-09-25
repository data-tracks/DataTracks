use crate::algebra::algebra::{Algebra, RefHandler, ValueEnumerator};
use crate::processing::Train;
use crate::value::Value;
use std::vec;

pub trait Scan: Algebra {}


#[derive(Clone)]
pub struct TrainScan {
    index: i64,
}

impl TrainScan {
    pub(crate) fn new(index: i64) -> Self {
        TrainScan { index }
    }
}

#[derive(Clone)]
pub struct ScanEnumerator {
    index: i64,
    values: Vec<Value>,
    trains: Vec<Train>,
}

impl ScanEnumerator {
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

impl Iterator for ScanEnumerator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() && !self.next_train() {
            return None
        }
        Some(self.values.remove(0))
    }
}

impl ValueEnumerator for ScanEnumerator {
    fn load(&mut self, trains: Vec<Train>) {
        for train in trains {
            if train.last == self.index {
                self.trains.push(train);
            }
        }
    }

    fn clone(&self) -> Box<dyn ValueEnumerator<Item=Value> + Send + 'static> {
        Box::new(ScanEnumerator { index: self.index, values: vec![], trains: self.trains.clone() })
    }
}

impl RefHandler for ScanEnumerator {
    fn process(&self, _stop: i64, wagons: Vec<Train>) -> Vec<Train> {
        let mut values = vec![];
        wagons.into_iter().filter(|w| w.last == self.index).for_each(|mut t| values.append(t.values.take().unwrap().as_mut()));
        vec![Train::new(self.index, values)]
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        Box::new(ScanEnumerator { index: self.index, values: vec![], trains: vec![] })
    }
}

impl Algebra for TrainScan {
    fn get_enumerator(&mut self) -> Box<dyn ValueEnumerator<Item=Value> + Send> {
        Box::new(ScanEnumerator { index: self.index, values: vec![], trains: vec![] })
    }
}

impl Scan for TrainScan {}

#[cfg(test)]
mod test {
    use crate::algebra::algebra::Algebra;
    use crate::algebra::scan::TrainScan;
    use crate::processing::Train;
    use crate::value::Dict;

    #[test]
    fn simple_scan() {
        let train = Train::new(0, Dict::transform(vec![3.into(), "test".into()]));

        let mut scan = TrainScan::new(0);

        let mut handler = scan.get_enumerator();
        handler.load(vec![train]);

        let mut train_2 = handler.drain_to_train(0);

        assert_eq!(train_2.values.clone().unwrap(), Dict::transform(vec![3.into(), "test".into()]));
        assert_ne!(train_2.values.take().unwrap(), Dict::transform(vec![8.into(), "test".into()]));
    }
}