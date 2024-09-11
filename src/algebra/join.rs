use crate::algebra::algebra::{Algebra, RefHandler};
use crate::algebra::AlgebraType;
use crate::processing::Train;
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
    left_hash: Option<fn(&Value) -> Hash>,
    right_hash: Option<fn(&Value) -> Hash>,
    out: Option<fn(Value, Value) -> Value>,
}

impl<H> TrainJoin<H>
where
    H: PartialEq,
{
    pub(crate) fn new(
        left: AlgebraType,
        right: AlgebraType,
        left_hash: fn(&Value) -> H,
        right_hash: fn(&Value) -> H,
        out: fn(Value, Value) -> Value,
    ) -> Self {
        TrainJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_hash: Some(left_hash),
            right_hash: Some(right_hash),
            out: Some(out),
        }
    }
}

pub struct JoinHandler<H>
where
    H: PartialEq + 'static,
{
    left_hash: fn(&Value) -> H,
    right_hash: fn(&Value) -> H,
    left: Box<dyn RefHandler>,
    right: Box<dyn RefHandler>,
    out: fn(Value, Value) -> Value,
}



impl<H> RefHandler for JoinHandler<H>
where
    H: PartialEq + 'static,
{
    fn process(&self, stop: i64, wagons: Vec<Train>) -> Train {
        let mut values = vec![];
        let mut left = self.left.process(stop, wagons.clone());
        let mut right = self.right.process(stop, wagons);
        let right_hashes: Vec<(H, Value)> = right.values.take().unwrap().into_iter().map(|value| {
            let hash = (self.right_hash)(&value);
            (hash, value)
        }).collect();
        for l_value in left.values.take().unwrap() {
            let l_hash = (self.left_hash)(&l_value);
            for (r_hash, r_val) in &right_hashes {
                if l_hash == *r_hash {
                    values.push((self.out)(l_value.clone(), r_val.clone()));
                }
            }
        }
        Train::new(stop, values)
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        Box::new(JoinHandler{
            left_hash: self.left_hash,
            right_hash: self.right_hash,
            left: self.left.clone(),
            right: self.right.clone(),
            out: self.out,
        })
    }
}

impl<H: PartialEq + 'static> Algebra for TrainJoin<H> {
    fn get_handler(&mut self) -> Box<dyn RefHandler + Send> {
        let left_hash = self.left_hash.take().unwrap();
        let right_hash = self.right_hash.take().unwrap();
        let out = self.out.take().unwrap();

        let left = self.left.get_handler();
        let right = self.right.get_handler();
        Box::new(JoinHandler { left_hash, right_hash, left, right, out })
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
    use crate::algebra::algebra::Algebra;
    use crate::algebra::join::TrainJoin;
    use crate::algebra::scan::TrainScan;
    use crate::algebra::AlgebraType::Scan;
    use crate::processing::Train;
    use crate::value::{Dict, Value};

    #[test]
    fn one_match() {
        let train0 = Train::new(0, Dict::transform(vec![3.into(), 5.5.into()]));
        let train1 = Train::new(1, Dict::transform(vec![5.5.into(), "test".into()]));

        let left = TrainScan::new(0);

        let right = TrainScan::new(1);

        let mut join = TrainJoin::new(Scan(left), Scan(right), |val| val.clone(), |val| val.clone(), |left, right| {
            Value::Dict(left.as_dict().unwrap().merge(right.as_dict().unwrap()))
        });

        let handle = join.get_handler();
        let mut res = handle.process(0, vec![train0, train1]);
        assert_eq!(res.values.clone().unwrap(), vec![Value::Dict(Dict::from(vec![5.5.into(), 5.5.into()]))]);
        assert_ne!(res.values.take().unwrap(), vec![Value::Dict(Dict::from(vec![]))]);
    }

    #[test]
    fn multi_match() {
        let train0 = Train::new(0, Dict::transform(vec![3.into(), 5.5.into()]));
        let train1 = Train::new(1, Dict::transform(vec![5.5.into(), 5.5.into()]));

        let left = TrainScan::new(0);
        let right = TrainScan::new(1);

        let mut join = TrainJoin::new(Scan(left), Scan(right), |val| val.clone(), |val| val.clone(), |left, right| {
            Value::Dict(left.as_dict().unwrap().merge(right.as_dict().unwrap()))
        });

        let handle = join.get_handler();
        let mut res = handle.process(0, vec![train0, train1]);
        assert_eq!(res.values.clone().unwrap(), vec![Value::Dict(Dict::from(vec![5.5.into(), 5.5.into()])), Value::Dict(Dict::from(vec![5.5.into(), 5.5.into()]))]);
        assert_ne!(res.values.take().unwrap(), vec![vec![].into()]);
    }
}