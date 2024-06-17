use std::sync::Arc;

use crate::algebra::algebra::Algebra;
use crate::algebra::AlgebraType;
use crate::processing::{Train, Referencer};
use crate::value::Value;

pub trait Project: Algebra {
    fn get_input(&self) -> &AlgebraType;
}

pub struct TrainProject {
    input: Box<AlgebraType>,
    project: Arc<Box<dyn Fn(Value) -> Value + Send + Sync + 'static>>,
}

impl Algebra for TrainProject {
    fn get_handler(&self) -> Referencer {
        let project = Arc::clone(&self.project);
        let input = self.input.get_handler();
        Box::new(move |train: &mut Train| {
            let mut train = input(train);
            let projected = train.values.get_mut(&0).unwrap().take().unwrap().into_iter().map(|value: Value| project(value)).collect();
            Train::default(projected)
        })
    }
}

impl Project for TrainProject {
    fn get_input(&self) -> &AlgebraType {
        &self.input
    }
}