use crate::algebra::function::{Implementable, Operator};
use core::BoxedValueHandler;

pub fn implement(function: &Operator) -> BoxedValueHandler {
    implement_func(function)
}

pub fn implement_func(function: &Operator) -> BoxedValueHandler {
    function.implement().unwrap()
}
