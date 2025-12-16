use std::cmp::Ordering;
use std::collections::HashMap;
use value::Value;

#[derive(Clone)]
pub enum ValueExtractor {
    Key(String),
}


impl ValueExtractor {

    pub fn extract(&self, a: &Value) -> Value {
        match self {
            ValueExtractor::Key(k) => {
                match a {
                    Value::Dict(d) => {
                        d.get(k).cloned().unwrap_or(Value::null())
                    },
                    a => a.clone()
                }
            }
        }
    }
}