use crate::algebra::algebra::{Algebra, RefHandler, ValueEnumerator};
use crate::processing::Train;
use crate::value::Value;
use std::vec;

pub trait Scan: Algebra {}

pub struct TrainScan {
    index: i64,
}

impl TrainScan {
    pub(crate) fn new(index: i64) -> Self {
        TrainScan { index }
    }

    pub fn next(self, trains: Vec<Train>) -> ScanEnumerator {
        ScanEnumerator {index: self.index, trains}
    }
}

#[derive(Clone)]
pub struct ScanEnumerator {
    index: i64,
    trains: Vec<Train>,
}

impl Iterator for ScanEnumerator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl ValueEnumerator for ScanEnumerator {
    fn load(&mut self, trains: Vec<Train>) {
        self.trains.append(&mut trains.clone())
    }
}

impl RefHandler for ScanEnumerator {
    fn process(&self, _stop: i64, wagons: Vec<Train>) -> Vec<Train> {
        let mut values = vec![];
        wagons.into_iter().filter(|w| w.last == self.index).for_each(|mut t| values.append(t.values.take().unwrap().as_mut()));
        vec![Train::new(self.index, values)]
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        Box::new(ScanEnumerator { index: self.index, trains: vec![] })
    }
}

impl Algebra for TrainScan {
    fn get_enumerator(&mut self) -> Box<dyn ValueEnumerator<Item=Value> + Send> {
        Box::new(ScanEnumerator { index: self.index, trains: vec![] })
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

        let handler = scan.get_enumerator();

        let mut train_2 = handler.process(0, vec![train]);

        assert_eq!(train_2.get(0).unwrap().values.clone().unwrap(), Dict::transform(vec![3.into(), "test".into()]));
        assert_ne!(train_2.get_mut(0).unwrap().values.take().unwrap(), Dict::transform(vec![8.into(), "test".into()]));
    }
}