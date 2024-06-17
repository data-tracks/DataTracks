use std::collections::HashMap;
use std::sync::Arc;

use crate::algebra::algebra::Algebra;
use crate::algebra::AlgebraType;
use crate::processing::{Train, Referencer};
use crate::value::Value;

pub trait Filter: Algebra {
    fn get_input(&self) -> &AlgebraType;
}

pub struct TrainFilter {
    input: Box<AlgebraType>,
    condition: Arc<Box<dyn Fn(&Value) -> bool + Sync + Send>>,
}

impl Algebra for TrainFilter {
    fn get_handler(&self) -> Referencer {
        let condition = Arc::clone(&self.condition);
        let input = self.input.get_handler();
        Box::new(move |train: &mut Train| {
            let mut train = input(train);
            let filtered = train.values.get_mut(&0).unwrap().take().unwrap().into_iter().filter(|v| condition(v)).collect();
            Train::default(filtered)
        })
    }
}

impl Filter for TrainFilter {
    fn get_input(&self) -> &AlgebraType {
        &self.input
    }
}