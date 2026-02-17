use std::collections::{BTreeMap, HashMap};
use mongodb::bson::{to_bson, Bson, DateTime, Document, Timestamp};
use crate::value::Value;

impl From<&Bson> for Value {
    fn from(bson: &Bson) -> Self {
        match bson {
            Bson::Double(d) => Value::float(*d),
            Bson::String(s) => Value::text(s.as_str()),
            Bson::Document(d) => d.into(),
            Bson::Boolean(b) => Value::bool(*b),
            Bson::Null => Value::null(),
            Bson::Int32(i) => Value::int(*i as i64),
            Bson::Int64(i) => Value::int(*i),
            Bson::Undefined => Value::null(),
            Bson::ObjectId(id) => Value::text(&id.to_string()),
            _ => todo!(),
        }
    }
}

impl From<Bson> for Value {
    fn from(bson: Bson) -> Self {
        (&bson).into()
    }
}

impl From<Value> for Bson {

    fn from(value: Value) -> Self {
        match value {
            Value::Int(i) => Bson::Int32(i.0 as i32),
            Value::Float(f) => Bson::Double(f.as_f64()),
            Value::Bool(b) => Bson::Boolean(b.0),
            Value::Text(t) => Bson::String(t.0),
            Value::Time(t) => Bson::Timestamp(Timestamp {
                time: t.ms as u32,
                increment: 0,
            }),
            Value::Date(d) => Bson::DateTime(DateTime::from_millis(d.as_epoch())),
            Value::Array(a) => Bson::Array(a.values.into_iter().map(|v| v.into()).collect()),
            Value::Dict(d) => to_bson(
                &d.into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect::<HashMap<String, Bson>>(),
            )
                .unwrap(),
            Value::Node(_) => todo!(),
            Value::Edge(_) => todo!(),
            Value::Null => Bson::Null,
        }
    }
}

impl From<&Document> for Value {
    fn from(doc: &Document) -> Self {
        let mut map: BTreeMap<String, Value> = BTreeMap::new();
        doc.iter().for_each(|(k, v)| {
            map.insert(k.to_string(), v.clone().into());
        });

        Value::dict(map)
    }
}

impl From<Document> for Value {
    fn from(doc: Document) -> Self {
        (&doc).into()
    }
}