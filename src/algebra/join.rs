use std::sync::Arc;

use crate::algebra::algebra::Algebra;
use crate::processing::Train;
use crate::value::Value;

pub trait Join: Algebra {
    fn left(&self) -> &dyn Algebra;
    fn right(&self) -> &dyn Algebra;
}

pub struct TrainJoin<'a> {
    left: &'a dyn Algebra,
    right: &'a dyn Algebra,
    left_hash: Arc<Box<dyn Fn(&Value) -> &str>>,
    right_hash: Arc<Box<dyn Fn(&Value) -> &str>>,
    out: Arc<Box<dyn Fn(Value, Value) -> Value>>,
}

impl TrainJoin {
    pub(crate) fn new(left: &dyn Algebra, right: &dyn Algebra, left_hash: Box<dyn Fn(&Value) -> &str>, right_hash: Box<dyn Fn(&Value) -> &str>, out: Box<dyn Fn(Value, Value) -> Value>) -> Self {
        TrainJoin {
            left,
            right,
            left_hash: Arc::new(left_hash),
            right_hash: Arc::new(right_hash),
            out: Arc::new(out),
        }
    }
}

impl Algebra for TrainJoin<'_> {
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
                let right_hashes: Vec<(&str, Value)> = right.values.iter().map(|value: &Value| (right_hash(value), value.clone())).collect();
                for (i, l_value) in left.values.iter().enumerate() {
                    let l_hash = left_hash(l_value);
                    for (j, r_hash) in right_hashes.iter().enumerate() {
                        if l_hash == (*r_hash).0 {
                            values.push(out(l_value.clone(), (*r_hash).1.clone()));
                        }
                    }
                }
                Train::new(values)
            }
        )
    }
}

impl Join for TrainJoin<'_> {
    fn left(&self) -> &dyn Algebra {
        self.left
    }

    fn right(&self) -> &dyn Algebra {
        self.right
    }
}

#[cfg(test)]
mod test {
    use std::hash::Hash;

    use crate::algebra::join::TrainJoin;
    use crate::algebra::scan::TrainScan;
    use crate::processing::Train;
    use crate::value::Value;

    #[test]
    fn simple() {
        let left = TrainScan::new(Train::new(vec![3.into(), 5.5.into()]));
        let right = TrainScan::new(Train::new(vec!["test".into(), 5.5.into()]));

        let join = TrainJoin::new(&left, &right, |val: Value| val.hash());
    }
}