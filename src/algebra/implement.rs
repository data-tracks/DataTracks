use crate::algebra::algebra::ValueHandler;
use crate::algebra::function::Operator;
use crate::algebra::BoxedValueHandler;

pub fn implement(function: &Operator) -> BoxedValueHandler {
    implement_func(function)
}

pub fn implement_func(function: &Operator) -> BoxedValueHandler {
    ValueHandler::clone(function)
}