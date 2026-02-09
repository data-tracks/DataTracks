use std::collections::HashMap;
use neo4rs::{BoltBoolean, BoltFloat, BoltInteger, BoltList, BoltMap, BoltNull, BoltString, BoltType, Row};
use crate::{Int, Text, Value};

impl From<Value> for BoltType {
    fn from(value: Value) -> Self {
        match value {
            Value::Int(i) => BoltType::Integer(BoltInteger::new(i.0)),
            Value::Float(f) => BoltType::Float(BoltFloat::new(f.as_f64())),
            Value::Bool(b) => BoltType::Boolean(BoltBoolean::new(b.0)),
            Value::Text(t) => BoltType::String(BoltString::new(&t.0)),
            Value::Node(n) => {
                let mut map = HashMap::<BoltString, BoltType>::new();
                map.insert(
                    BoltString::new("id"),
                    BoltType::Integer(BoltInteger::new(n.id.0)),
                );
                map.insert(
                    BoltString::new("labels"),
                    BoltType::List(BoltList {
                        value: n
                            .labels
                            .into_iter()
                            .map(|l| BoltType::String(BoltString::new(&l.0)))
                            .collect(),
                    }),
                );
                map.insert(
                    BoltString::new("props"),
                    BoltType::Map(BoltMap {
                        value: n
                            .properties
                            .into_iter()
                            .map(|(k, v)| (BoltString::new(k.as_str()), BoltType::from(v)))
                            .collect::<HashMap<_, _>>(),
                    }),
                );

                BoltType::Map(BoltMap { value: map })
            }
            Value::Dict(d) => BoltType::Map(BoltMap {
                value: d
                    .values
                    .into_iter()
                    .map(|(k, v)| (BoltString::new(k.as_str()), BoltType::from(v)))
                    .collect(),
            }),
            Value::Null => BoltType::Null(BoltNull),
            Value::Array(a) => BoltType::List(BoltList {
                value: a.values.into_iter().map(|v| v.into()).collect(),
            }),
            v => todo!("{}", v),
        }
    }
}

impl From<Text> for BoltType {
    fn from(value: Text) -> Self {
        BoltType::String(BoltString::new(&value.0))
    }
}

impl From<Int> for BoltType {
    fn from(value: Int) -> Self {
        BoltType::Integer(BoltInteger::new(value.0))
    }
}

impl From<Row> for Value {
    fn from(row: Row) -> Self {
        let mut values = vec![];
        for key in row.keys() {
            values.push(row.get::<Value>(&key.value).unwrap())
        }
        Value::array(values)
    }
}