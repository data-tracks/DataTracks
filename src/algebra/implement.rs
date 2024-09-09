use crate::algebra::function::Function;
use crate::value::Value;

pub fn implement(function: &Function) -> fn(Value) -> Value{
    let func = implement_func(function);
    func
}

pub fn implement_func(function: &Function) -> fn(Value) -> Value {
    match function {
        Function::Literal(l) => {
            |v| l.literal
        }
        Function::NamedRef(n) => {
            |v| {
                match v {
                    Value::Dict(d) => d.0.get(&n.name).unwrap().clone(),
                    Value::Null(_) => Value::null(),
                    _ => panic!()
                }
            }
        }
        Function::IndexedRef(i) => {
            |v| {
                match v {
                    Value::Array(a) => a.0.get(i.index).cloned().unwrap(),
                    Value::Null(_) => Value::null(),
                    _ => panic!()
                }
            }
        }
        Function::Operation(o) => {
            o.op.implement(o.operands.iter().map(|v| implement_func(v)).collect())
        }
    }
}