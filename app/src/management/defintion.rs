use std::collections::HashMap;
use value::Value;

/// Defines how values are constraint over data entries
/// do not to store everything all the time, need more control which aspects to select
///
/// (should these definitions led to as close of operations of engines as possible?)
struct Definition {
    values: HashMap<u64, Box<dyn Fn(Value) -> Value>>,
    relations: Vec<(Vec<u64>, Constraint)>,
}


enum Constraint {

}