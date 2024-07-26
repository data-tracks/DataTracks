use crate::algebra::algebra::{Algebra, RefHandler};
use crate::processing::Train;

pub trait Scan: Algebra {}

pub struct TrainScan {
    index: i64,
}

impl TrainScan {
    pub(crate) fn new(index: i64) -> Self {
        TrainScan { index }
    }
}

#[derive(Clone)]
pub struct ScanHandler {
    index: i64,
}

impl RefHandler for ScanHandler {
    fn process(&self, _stop: i64, wagons: Vec<Train>) -> Train {
        let mut values = vec![];
        wagons.into_iter().filter(|w| w.last == self.index).for_each(|mut t| values.append(t.values.take().unwrap().as_mut()));
        Train::new(self.index, values)
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        Box::new(ScanHandler{ index: 0 })
    }
}

impl Algebra for TrainScan {
    fn get_handler(&mut self) -> Box<dyn RefHandler + Send> {
        Box::new(ScanHandler { index: self.index })
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

        let handler = scan.get_handler();

        let mut train_2 = handler.process(0, vec![train]);

        assert_eq!(train_2.values.clone().unwrap(), Dict::transform(vec![3.into(), "test".into()]));
        assert_ne!(train_2.values.take().unwrap(), Dict::transform(vec![8.into(), "test".into()]));
    }
}