use crate::algebra::algebra::ValueHandler;
use crate::algebra::function::Function;

pub fn implement(function: &Function) -> Box<dyn ValueHandler + Send> {
    let func = implement_func(function);
    func
}

pub fn implement_func(function: &Function) -> Box<dyn ValueHandler + Send> {
    ValueHandler::clone(function)
}