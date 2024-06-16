use std::sync::Arc;

use crate::algebra::algebra::Algebra;
use crate::processing::Train;
use crate::value::Value;

pub trait Join: Algebra {
    fn left(&self) -> &dyn Algebra;
    fn right(&self) -> &dyn Algebra;
}

pub struct TrainJoin<'a, Hash>
where
    Hash: PartialEq,
{
    left: &'a dyn Algebra,
    right: &'a dyn Algebra,
    left_hash: Arc<Box<dyn Fn(&Value) -> &Hash>>,
    right_hash: Arc<Box<dyn Fn(&Value) -> &Hash>>,
    out: Arc<Box<dyn Fn(Value, Value) -> Value>>,
}

impl<'a, H> TrainJoin<'a, H>
where
    H: PartialEq,
{
    pub(crate) fn new(
        left: &'a dyn Algebra,
        right: &'a dyn Algebra,
        left_hash: Box<dyn Fn(&Value) -> &H>,
        right_hash: Box<dyn Fn(&Value) -> &H>,
        out: Box<dyn Fn(Value, Value) -> Value>,
    ) -> Self {
        TrainJoin {
            left,
            right,
            left_hash: Arc::new(left_hash),
            right_hash: Arc::new(right_hash),
            out: Arc::new(out),
        }
    }
}

impl<H: PartialEq + 'static> Algebra for TrainJoin<'_, H> {
    fn get_handler(&self) -> Box<dyn Fn() -> Train> {
        let left_hash = Arc::clone(&self.left_hash);
        let right_hash = Arc::clone(&self.right_hash);
        let out = Arc::clone(&self.out);

        let left = self.left.get_handler();
        let right = self.right.get_handler();

        Box::new(
            move || {
                let mut values = vec![];
                let left = left();
                let right = right();
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

impl<H: PartialEq + 'static> Join for TrainJoin<'_, H> {
    fn left(&self) -> &dyn Algebra {
        self.left
    }

    fn right(&self) -> &dyn Algebra {
        self.right
    }
}

#[cfg(test)]
mod test {
    use crate::algebra::algebra::Algebra;
    use crate::algebra::join::TrainJoin;
    use crate::algebra::scan::TrainScan;
    use crate::processing::Train;
    use crate::value::Value;

    #[test]
    fn one_match() {
        let left = TrainScan::new(Train::single(vec![3.into(), 5.5.into()]));
        let right = TrainScan::new(Train::single(vec![5.5.into(), "test".into()]));

        let join = TrainJoin::new(&left, &right, Box::new(|val: &Value| val), Box::new(|val: &Value| val), Box::new(|left: Value, right: Value| {
            vec![left.into(), right.into()].into()
        }));

        let handle = join.get_handler();
        let res = handle();
        assert_eq!(res.values.get(&0).unwrap(), &vec![vec![5.5.into(), 5.5.into()].into()]);
        assert_ne!(res.values.get(&0).unwrap(), &vec![vec![].into()]);
    }

    #[test]
    fn multi_match() {
        let left = TrainScan::new(Train::single(vec![3.into(), 5.5.into()]));
        let right = TrainScan::new(Train::single(vec![5.5.into(), 5.5.into()]));

        let join = TrainJoin::new(&left, &right, Box::new(|val: &Value| val), Box::new(|val: &Value| val), Box::new(|left: Value, right: Value| {
            vec![left.into(), right.into()].into()
        }));

        let handle = join.get_handler();
        let res = handle();
        assert_eq!(res.values.get(&0).unwrap(), &vec![vec![5.5.into(), 5.5.into()].into(), vec![5.5.into(), 5.5.into()].into()]);
        assert_ne!(res.values.get(&0).unwrap(), &vec![vec![].into()]);
    }
}