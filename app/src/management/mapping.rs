use value::Value;

/// How a values is transformed into another one
/// only data format
///
/// depending on mapping complexity and also throughput is determent
/// also influences space used (more efficient storage)
pub struct Mapping {
    func: Box<dyn Fn(Value) -> Value>,
}
