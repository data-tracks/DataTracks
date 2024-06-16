use std::sync::Arc;

use crate::algebra::algebra::Algebra;
use crate::processing::Train;
use crate::value::Value;

pub trait Filter: Algebra {
    fn get_input(&self) -> &dyn Algebra;
}

pub struct TrainFilter<'a> {
    input: &'a dyn Algebra,
    condition: Arc<Box<dyn Fn(&Value) -> bool>>,
}

impl Algebra for TrainFilter<'_> {
    fn get_handler(&self) -> Box<dyn Fn() -> Train> {
        let condition = Arc::clone(&self.condition);
        let input = self.input.get_handler();
        Box::new(move || {
            let train = input();
            let filtered = train.values.get(&0).unwrap().into_iter().filter(|v| condition(v)).cloned().collect();
            Train::single(filtered)
        })
    }
}

impl Filter for TrainFilter<'_> {
    fn get_input(&self) -> &dyn Algebra {
        self.input
    }
}