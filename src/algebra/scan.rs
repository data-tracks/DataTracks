use crate::algebra::algebra::Algebra;
use crate::processing::Train;

pub trait Scan: Algebra {}

pub struct TrainScan {
    train: Train,
}

impl TrainScan {
    pub(crate) fn new(train: Train) -> Self {
        TrainScan { train }
    }
}

impl Algebra for TrainScan {
    fn get_handler(&self) -> Box<dyn Fn() -> Train> {
        let train = self.train.clone();
        Box::new(move || {
            train.clone()
        })
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
        let train = Train::single(vec![3.into(), "test".into()]);

        let scan = TrainScan::new(train);

        let handler = scan.get_handler();

        let train_2 = handler();

        assert_eq!(train_2.values.get(&0).unwrap(), &vec![3.into(), "test".into()]);
        assert_ne!(train_2.values.get(&0).unwrap(), &vec![8.into(), "test".into()]);
    }
}