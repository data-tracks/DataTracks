use crate::algebra::algebra::Algebra;
use crate::algebra::{AlgebraType, ValueEnumerator};
use crate::processing::Train;
use crate::value::Value;

pub trait Join: Algebra {
    fn left(&self) -> &AlgebraType;
    fn right(&self) -> &AlgebraType;
}

pub struct TrainJoin {
    left: Box<AlgebraType>,
    right: Box<AlgebraType>,
    left_hash: Option<fn(&Value) -> Value>,
    right_hash: Option<fn(&Value) -> Value>,
    out: Option<fn(Value, Value) -> Value>,
}

impl TrainJoin {
    pub(crate) fn new(
        left: AlgebraType,
        right: AlgebraType,
        left_hash: fn(&Value) -> Value,
        right_hash: fn(&Value) -> Value,
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

pub struct JoinHandler {
    left_hash: fn(&Value) -> Value,
    right_hash: fn(&Value) -> Value,
    left: Box<dyn ValueEnumerator<Item=Value> + Send>,
    right: Box<dyn ValueEnumerator<Item=Value> + Send>,
    out: fn(Value, Value) -> Value,
    cache_left: Vec<(Value, Value)>,
    cache_right: Vec<(Value, Value)>,
    left_index: usize,
    right_index: usize,
}

impl JoinHandler {
    pub(crate) fn new(left_hash: fn(&Value) -> Value, right_hash: fn(&Value) -> Value, output: fn(Value, Value) -> Value, left: Box<dyn ValueEnumerator<Item=Value> + Send>, right: Box<dyn ValueEnumerator<Item=Value> + Send>) -> Self {
        JoinHandler {
            left_hash,
            right_hash,
            left,
            right,
            out: output,
            cache_left: vec![],
            cache_right: vec![],
            left_index: 0,
            right_index: 0,
        }
    }
}

impl Iterator for JoinHandler {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {

        // only first time
        if self.cache_left.is_empty() && !self.next_left() {
            return None;
        }

        loop {
            if !self.next_right() {
                if !self.next_left() {
                    return None; // cannot advance further
                }
                self.right_index = 0;
            }

            let left = &self.cache_left.get(self.left_index)?;
            let right = &self.cache_right.get(self.right_index)?;


            if left.0 == right.0 {
                return Some((self.out)(left.1.clone(), right.1.clone()));
            }
        }
    }
}

impl JoinHandler {
    fn next_left(&mut self) -> bool {
        if let Some(val) = self.left.next() {
            self.cache_left.push(((self.left_hash)(&val.clone()), val));
            if self.cache_left.len() > 1 {
                self.left_index += 1;
            }
            self.right_index = 0; // we reset right
            true
        } else {
            if self.left_index < self.cache_left.len() - 1 {
                self.left_index += 1;
                self.right_index = 0;

                true
            } else {
                false
            }
        }
    }

    fn next_right(&mut self) -> bool {
        if let Some(val) = self.right.next() {
            self.cache_right.push(((self.right_hash)(&val.clone()), val));
            if self.cache_right.len() > 1 {
                self.right_index += 1;
            }
            true
        } else {
            if self.right_index < self.cache_right.len() - 1 { // index 0 length 1 cannot go further
                self.right_index += 1;

                true
            } else {
                false
            }
        }
    }
}

impl ValueEnumerator for JoinHandler {
    fn load(&mut self, trains: Vec<Train>) {
        self.left.load(trains.clone());
        self.right.load(trains);
    }

    fn clone(&self) -> Box<dyn ValueEnumerator<Item=Value> + Send + 'static> {
        Box::new(JoinHandler::new(self.left_hash, self.right_hash, self.out, self.left.clone(), self.right.clone()))
    }
}

impl Algebra for TrainJoin {
    fn get_enumerator(&mut self) -> Box<dyn ValueEnumerator<Item=Value> + Send> {
        let left_hash = self.left_hash.take().unwrap();
        let right_hash = self.right_hash.take().unwrap();
        let out = self.out.take().unwrap();

        let left = self.left.get_enumerator();
        let right = self.right.get_enumerator();
        Box::new(JoinHandler::new(left_hash, right_hash, out, left, right))
    }
}

impl Join for TrainJoin {
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
        let left = Train::new(0, Dict::transform(vec![3.into(), 5.5.into()]));
        let right = Train::new(1, Dict::transform(vec![5.5.into(), "test".into()]));

        let left_scan = TrainScan::new(0);

        let right_scan = TrainScan::new(1);

        let mut join = TrainJoin::new(Scan(left_scan), Scan(right_scan), |val| val.clone(), |val| val.clone(), |left, right| {
            Value::Dict(left.as_dict().unwrap().merge(right.as_dict().unwrap()))
        });

        let mut handle = join.get_enumerator();
        handle.load(vec![left]);
        handle.load(vec![right]);
        let mut res = handle.drain_to_train(3);
        assert_eq!(res.clone().values.unwrap(), vec![Value::Dict(Dict::from(vec![5.5.into(), 5.5.into()]))]);
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

        let mut handle = join.get_enumerator();
        handle.load(vec![train0.clone(), train1.clone()]);
        let mut res = handle.drain_to_train(3);
        assert_eq!(res.values.clone().unwrap(), vec![Value::Dict(Dict::from(vec![5.5.into(), 5.5.into()])), Value::Dict(Dict::from(vec![5.5.into(), 5.5.into()]))]);
        assert_ne!(res.values.take().unwrap(), vec![vec![].into()]);
    }
}