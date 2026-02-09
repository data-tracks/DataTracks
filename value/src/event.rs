
use mongodb::bson::Document;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::collections::BTreeMap;
use crate::value::Value;

#[derive(Debug, Clone, Deserialize, Serialize, Writable, Readable)]
pub enum Event {
    Insert(InsertEvent),
    Update(UpdateEvent),
    Delete(DeleteEvent),
    Begin,
    End,
    Other,
}


impl From<Event> for Value {
    fn from(event: Event) -> Self {
        match event {
            Event::Insert(i) => {
                let mut map = BTreeMap::new();
                map.insert("value".to_string(), i.value);
                map.insert("type".to_string(), Value::text("insert"));
                Value::dict(map)
            }
            Event::Update(u) => {
                let mut map = BTreeMap::new();
                map.insert("identity".to_string(), u.identity);
                map.insert("value".to_string(), u.value);
                map.insert("type".to_string(), Value::text("update"));
                Value::dict(map)
            }
            Event::Delete(d) => {
                let mut map = BTreeMap::new();
                map.insert("identity".to_string(), d.identity);
                map.insert("type".to_string(), Value::text("delete"));
                Value::dict(map)
            }
            Event::Begin => Value::text("begin"),
            Event::End => Value::text("commit"),
            Event::Other => Value::text("other"),
        }
    }
}


impl From<ChangeStreamEvent<Document>> for Event {
    fn from(event: ChangeStreamEvent<Document>) -> Self {
        //println!("{:?}", event);
        match event.operation_type {
            OperationType::Insert => Event::Insert(InsertEvent {
                value: {
                    Value::dict(BTreeMap::from_iter(
                        event
                            .full_document
                            .unwrap_or_default()
                            .into_iter()
                            .map(|(key, value)| (key, value.into())),
                    ))
                },
            }),
            OperationType::Update => {
                let description = event.update_description.unwrap();
                Event::Update(UpdateEvent {
                    identity: Value::dict_from_kv(
                        "_id",
                        event.document_key.unwrap().get("_id").unwrap().into(),
                    ),
                    value: Value::dict(BTreeMap::from_iter(
                        description
                            .updated_fields
                            .into_iter()
                            .map(|(key, value)| (key, value.into())),
                    )),
                })
            }
            OperationType::Replace => todo!(),
            OperationType::Delete => Event::Delete(DeleteEvent {
                identity: Value::dict_from_kv(
                    "_id",
                    event.document_key.unwrap().get("_id").unwrap().into(),
                ),
            }),
            OperationType::Drop => todo!(),
            OperationType::Rename => todo!(),
            OperationType::DropDatabase => todo!(),
            OperationType::Invalidate => todo!(),
            OperationType::Other(_) => Event::Other,
            _ => todo!(),
        }
    }
}

impl From<serde_json::Value> for Event {
    fn from(value: serde_json::Value) -> Self {
        match value
            .get("action")
            .unwrap()
            .as_str()
            .unwrap()
            .to_lowercase()
            .as_ref()
        {
            "i" => Event::Insert(InsertEvent {
                value: Self::extract_values_for_key("columns", value),
            }),
            "u" => Event::Update(UpdateEvent {
                identity: Self::extract_values_for_key("identity", value.clone()),
                value: Self::extract_values_for_key("columns", value),
            }),
            "d" => Event::Delete(DeleteEvent {
                identity: Self::extract_values_for_key("identity", value.clone()),
            }),
            "b" => Event::Begin,
            "c" => Event::End,
            _ => panic!("error on from json: type"),
        }
    }
}

impl Event {
    fn extract_values_for_key<S: AsRef<str>>(key: S, value: serde_json::Value) -> Value {
        Value::dict(BTreeMap::from_iter(
            value
                .get(key.as_ref())
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|v| {
                    (
                        v.get("name")
                            .unwrap()
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        v.get("value").unwrap().into(),
                    )
                }),
        ))
    }
}

#[derive(Debug)]
pub enum CdcEventType {
    Insert,
    Update,
    Delete,
    Other,
}

#[derive(Debug, Clone, Deserialize, Serialize, Writable, Readable)]
pub struct InsertEvent {
    pub value: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize, Writable, Readable)]
pub struct UpdateEvent {
    pub identity: Value,
    pub value: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize, Writable, Readable)]
pub struct DeleteEvent {
    pub identity: Value,
}

impl From<OperationType> for CdcEventType {
    fn from(operation_type: OperationType) -> Self {
        match operation_type {
            OperationType::Insert => CdcEventType::Insert,
            OperationType::Update => CdcEventType::Update,
            OperationType::Delete => CdcEventType::Delete,
            _ => CdcEventType::Other,
        }
    }
}
