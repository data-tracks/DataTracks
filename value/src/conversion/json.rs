use std::collections::BTreeMap;
use json::JsonValue;
use crate::{Dict};
use crate::value::Value;

impl From<&serde_json::Map<String, serde_json::Value>> for Value {
    fn from(value: &serde_json::Map<String, serde_json::Value>) -> Self {
        Value::Dict(value.into())
    }
}

impl From<&serde_json::Map<String, serde_json::Value>> for Dict {
    fn from(value: &serde_json::Map<String, serde_json::Value>) -> Self {
        let mut map = BTreeMap::new();
        for (key, value) in value {
            map.insert(key.clone(), value.into());
        }
        Dict::new(map)
    }
}

impl From<&serde_json::Value> for Value {
    fn from(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Value::null(),
            serde_json::Value::Bool(b) => Value::bool(*b),
            serde_json::Value::Number(n) => {
                if n.is_f64() {
                    Value::float(n.as_f64().unwrap())
                } else {
                    Value::int(n.as_i64().unwrap())
                }
            }
            serde_json::Value::String(s) => Value::text(s),
            serde_json::Value::Array(a) => {
                let mut values = vec![];
                for value in a {
                    values.push(value.into());
                }
                Value::array(values)
            }
            serde_json::Value::Object(o) => o.into(),
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Self {
        (&value).into()
    }
}

impl From<&JsonValue> for Value {
    fn from(value: &JsonValue) -> Self {
        match value {
            JsonValue::Null => Value::null(),
            JsonValue::Short(a) => Value::text(a.as_str()),
            JsonValue::String(a) => Value::text(a),
            JsonValue::Number(a) => Value::int(a.as_fixed_point_i64(0).unwrap()),
            JsonValue::Boolean(a) => Value::bool(*a),
            JsonValue::Object(elements) => Value::dict(
                elements
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.into()))
                    .collect(),
            ),
            JsonValue::Array(elements) => Value::array(
                elements
                    .iter()
                    .map(|arg0: &JsonValue| arg0.into())
                    .collect(),
            ),
        }
    }
}