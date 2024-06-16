use std::sync::Arc;

use crate::algebra::algebra::Algebra;
use crate::algebra::AlgebraType;
use crate::processing::{Train, Transformer};
use crate::value::Value;

pub trait Filter: Algebra {
    fn get_input(&self) -> &AlgebraType;
}

pub struct TrainFilter {
    input: Box<AlgebraType>,
    condition: Arc<Box<dyn Fn(&Value) -> bool>>,
}

impl Algebra for TrainFilter {
    fn get_handler(&self) -> Transformer {
        let condition = Arc::clone(&self.condition);
        let input = self.input.get_handler();
        Transformer(Box::new(move |train: Train| {
            let train = input.0(train);
            let filtered = train.values.get(&0).unwrap().into_iter().filter(|v| condition(v)).cloned().collect();
            Train::single(filtered)
        }))
    }
}

impl Filter for TrainFilter {
    fn get_input(&self) -> &AlgebraType {
        &self.input
    }
}