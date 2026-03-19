use crate::Dict;
use crate::value::Value;
use mongodb::bson::Document;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::collections::HashMap;

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
            Event::Insert(i) => Value::from((
                vec!["value".to_string(), "type".to_string()],
                vec![i.value, Value::text("insert")],
            )),
            Event::Update(u) => Value::from((
                vec![
                    "identity".to_string(),
                    "value".to_string(),
                    "type".to_string(),
                ],
                vec![u.identity, u.value, Value::text("update")],
            )),
            Event::Delete(d) => Value::from((
                vec!["identity".to_string(), "type".to_string()],
                vec![d.identity, Value::text("update")],
            )),
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
                    Value::dict(HashMap::from_iter(
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
                    identity: Dict::from((
                        "_id",
                        event.document_key.unwrap().get("_id").unwrap().into(),
                    ))
                    .into(),
                    value: Value::dict(HashMap::from_iter(
                        description
                            .updated_fields
                            .into_iter()
                            .map(|(key, value)| (key, value.into())),
                    )),
                })
            }
            OperationType::Replace => todo!(),
            OperationType::Delete => Event::Delete(DeleteEvent {
                identity: Dict::from((
                    "_id",
                    event.document_key.unwrap().get("_id").unwrap().into(),
                ))
                .into(),
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
        Value::dict(HashMap::from_iter(
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
