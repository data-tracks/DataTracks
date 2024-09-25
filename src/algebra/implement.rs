use crate::algebra::algebra::ValueHandler;
use crate::algebra::function::Function;

pub fn implement(function: &Function) -> Box<dyn ValueHandler + Send> {
    implement_func(function)
}

pub fn implement_func(function: &Function) -> Box<dyn ValueHandler + Send> {
    ValueHandler::clone(function)
}