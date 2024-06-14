use std::sync::Arc;

use crate::algebra::algebra::Algebra;
use crate::processing::Train;
use crate::value::Value;

pub trait Project: Algebra {
    fn get_input(&self) -> &dyn Algebra;
}

pub struct TrainProject<'a> {
    input: &'a dyn Algebra,
    project: Arc<Box<dyn Fn(Value) -> Value>>,
}

impl Algebra for TrainProject<'_> {
    fn get_handler(&self) -> Box<dyn Fn() -> Train> {
        let project = Arc::clone(&self.project);
        let input = self.input.get_handler();
        Box::new(move || {
            let train = input();
            let projected = train.values.into_iter().map(|value: Value| project(value)).collect();
            Train::new(projected)
        })
    }
}

impl Project for TrainProject<'_> {
    fn get_input(&self) -> &dyn Algebra {
        self.input
    }
}