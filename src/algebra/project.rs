use std::sync::Arc;

use crate::algebra::algebra::Algebra;
use crate::algebra::AlgebraType;
use crate::processing::{Train, Transformer};
use crate::value::Value;

pub trait Project: Algebra {
    fn get_input(&self) -> &AlgebraType;
}

pub struct TrainProject {
    input: Box<AlgebraType>,
    project: Arc<Box<dyn Fn(Value) -> Value + Send + Sync + 'static>>,
}

impl Algebra for TrainProject {
    fn get_handler(&self) -> Transformer {
        let project = Arc::clone(&self.project);
        let input = self.input.get_handler();
        Box::new(move |train: Train| {
            let train = input(train);
            let projected = train.values.get(&0).unwrap().into_iter().map(|value: &Value| project(value.clone())).collect();
            Train::single(projected)
        })
    }
}

impl Project for TrainProject {
    fn get_input(&self) -> &AlgebraType {
        &self.input
    }
}