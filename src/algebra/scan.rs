use crate::algebra::algebra::Algebra;
use crate::processing::{Train, Transformer};

pub trait Scan: Algebra {}

pub struct TrainScan {
    index: i64,
}

impl TrainScan {
    pub(crate) fn new(index: i64) -> Self {
        TrainScan { index }
    }
}

impl Algebra for TrainScan {
    fn get_handler(&self) -> Transformer {
        Box::new(move |train: Train| {
            train
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

        let scan = TrainScan::new(0);

        let handler = scan.get_handler();

        let train_2 = handler(train);

        assert_eq!(train_2.values.get(&0).unwrap(), &vec![3.into(), "test".into()]);
        assert_ne!(train_2.values.get(&0).unwrap(), &vec![8.into(), "test".into()]);
    }
}