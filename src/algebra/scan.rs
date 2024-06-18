use crate::algebra::algebra::{Algebra, Handler, RefHandler};
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
    fn process(&self, train: &mut Train) -> Train {
        Train::default(train.values.get_mut(&self.index).unwrap().take().unwrap())
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
        let mut train = Train::default(vec![3.into(), "test".into()]);

        let mut scan = TrainScan::new(0);

        let handler = scan.get_handler();

        let mut train_2 = handler.process(&mut train);

        assert_eq!(train_2.values.get(&0).unwrap().clone().unwrap(), vec![3.into(), "test".into()]);
        assert_ne!(train_2.values.get_mut(&0).unwrap().take().unwrap(), vec![8.into(), "test".into()]);
    }
}