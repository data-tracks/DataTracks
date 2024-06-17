use std::sync::Arc;

use crate::algebra::algebra::Algebra;
use crate::algebra::AlgebraType;
use crate::processing::{Train, Transformer};
use crate::value::Value;

pub trait Join: Algebra {
    fn left(&self) -> &AlgebraType;
    fn right(&self) -> &AlgebraType;
}

pub struct TrainJoin<Hash>
where
    Hash: PartialEq,
{
    left: Box<AlgebraType>,
    right: Box<AlgebraType>,
    left_hash: Arc<Box<dyn Fn(&Value) -> &Hash + Send + Sync>>,
    right_hash: Arc<Box<dyn Fn(&Value) -> &Hash + Send + Sync>>,
    out: Arc<Box<dyn Fn(Value, Value) -> Value + Send + Sync>>,
}

impl<H> TrainJoin<H>
where
    H: PartialEq,
{
    pub(crate) fn new(
        left: AlgebraType,
        right: AlgebraType,
        left_hash: Box<dyn Fn(&Value) -> &H + Send + Sync>,
        right_hash: Box<dyn Fn(&Value) -> &H + Send + Sync>,
        out: Box<dyn Fn(Value, Value) -> Value + Send + Sync>,
    ) -> Self {
        TrainJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_hash: Arc::new(left_hash),
            right_hash: Arc::new(right_hash),
            out: Arc::new(out),
        }
    }
}

impl<H: PartialEq + 'static> Algebra for TrainJoin<H> {
    fn get_handler(&self) -> Transformer {
        let left_hash = Arc::clone(&self.left_hash);
        let right_hash = Arc::clone(&self.right_hash);
        let out = Arc::clone(&self.out);

        let left = self.left.get_handler();
        let right = self.right.get_handler();

        Box::new(
            move |train: Train| {
                let mut values = vec![];
                let left = left(train.clone());
                let right = right(train);
                let right_hashes: Vec<(&H, Value)> = right.values.get(&0).unwrap().iter().map(|value: &Value| (right_hash(value), value.clone())).collect();
                for l_value in left.values.get(&0).unwrap() {
                    let l_hash = left_hash(&l_value);
                    for (r_hash, r_val) in &right_hashes {
                        if l_hash == *r_hash {
                            values.push(out(l_value.clone(), r_val.clone()));
                        }
                    }
                }
                Train::single(values)
            }
        )
    }
}

impl<H: PartialEq + 'static> Join for TrainJoin<H> {
    fn left(&self) -> &AlgebraType {
        &self.left
    }

    fn right(&self) -> &AlgebraType {
        &self.right
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::algebra::algebra::Algebra;
    use crate::algebra::AlgebraType::Scan;
    use crate::algebra::join::TrainJoin;
    use crate::algebra::scan::TrainScan;
    use crate::processing::Train;
    use crate::value::Value;

    #[test]
    fn one_match() {
        let train = Train::new(HashMap::from([
            (0, vec![3.into(), 5.5.into()]),
            (1, vec![5.5.into(), "test".into()])
        ]));

        let left = TrainScan::new(0);

        let right = TrainScan::new(1);

        let join = TrainJoin::new(Scan(left), Scan(right), Box::new(|val: &Value| val), Box::new(|val: &Value| val), Box::new(|left: Value, right: Value| {
            vec![left.into(), right.into()].into()
        }));

        let handle = join.get_handler();
        let res = handle(train);
        assert_eq!(res.values.get(&0).unwrap(), &vec![vec![5.5.into(), 5.5.into()].into()]);
        assert_ne!(res.values.get(&0).unwrap(), &vec![vec![].into()]);
    }

    #[test]
    fn multi_match() {
        let train = Train::new(HashMap::from([
            (0, vec![3.into(), 5.5.into()]),
            (1, vec![5.5.into(), 5.5.into()])
        ]));
        let left = TrainScan::new(0);
        let right = TrainScan::new(1);

        let join = TrainJoin::new(Scan(left), Scan(right), Box::new(|val: &Value| val), Box::new(|val: &Value| val), Box::new(|left: Value, right: Value| {
            vec![left.into(), right.into()].into()
        }));

        let handle = join.get_handler();
        let res = handle(train);
        assert_eq!(res.values.get(&0).unwrap(), &vec![vec![5.5.into(), 5.5.into()].into(), vec![5.5.into(), 5.5.into()].into()]);
        assert_ne!(res.values.get(&0).unwrap(), &vec![vec![].into()]);
    }
}