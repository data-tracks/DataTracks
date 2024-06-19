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

pub struct ScanHandler {
    index: i64,
}

impl RefHandler for ScanHandler {
    fn process(&self, stop: i64, wagons: &mut Vec<Train>) -> Train {
        let mut values = vec![];
        wagons.into_iter().filter(|w| w.last == self.index).for_each(|t| values.append(t.values.take().unwrap().as_mut()));
        Train::new(self.index, values)
    }
}

impl Algebra for TrainScan {
    fn get_handler(&mut self) -> Box<dyn RefHandler> {
        Box::new(ScanHandler { index: self.index })
    }
}

impl Scan for TrainScan {}

#[cfg(test)]
mod test {
    use crate::algebra::algebra::Algebra;
    use crate::algebra::scan::TrainScan;
    use crate::processing::Train;

    #[test]
    fn simple_scan() {
        let mut train = Train::new(0, vec![3.into(), "test".into()]);

        let mut scan = TrainScan::new(0);

        let handler = scan.get_handler();

        let mut train_2 = handler.process(0, &mut vec![train]);

        assert_eq!(train_2.values.clone().unwrap(), vec![3.into(), "test".into()]);
        assert_ne!(train_2.values.take().unwrap(), vec![8.into(), "test".into()]);
    }
}