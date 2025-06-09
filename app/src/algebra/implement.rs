use crate::algebra::function::{Implementable, Operator};
use crate::algebra::BoxedValueHandler;

pub fn implement(function: &Operator) -> BoxedValueHandler {
    implement_func(function)
}

pub fn implement_func(function: &Operator) -> BoxedValueHandler {
    function.implement().unwrap()
}
