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
    left_keys: Arc<Box<dyn Fn(Value) -> Value>>,
    right_keys: Arc<Box<dyn Fn(Value) -> Value>>,
    left_keys_out: Arc<Box<dyn Fn(Value) -> Value>>,
    right_keys_out: Arc<Box<dyn Fn(Value) -> Value>>,
}

impl Algebra for TrainJoin<'_> {
    fn get_handler(&self) -> Box<dyn Fn() -> Train> {
        let left_keys = Arc::clone(&self.left_keys);
        let right_keys = Arc::clone(&self.right_keys);
        let left_keys_out = Arc::clone(&self.left_keys_out);
        let right_keys_out = Arc::clone(&self.right_keys_out);

        let left = self.left.get_handler();
        let right = self.right.get_handler();

        Box::new(
            move || {
                todo!()
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