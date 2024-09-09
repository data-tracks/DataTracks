use crate::algebra::function::Function;
use crate::value::Value;

pub fn implement(function: &Function) -> fn(Value) -> Value{
    match function {
        Function::Literal(_) => {}
        Function::NamedRef(_) => {}
        Function::IndexedRef(_) => {}
        Function::Operation(_) => {}
    }
}