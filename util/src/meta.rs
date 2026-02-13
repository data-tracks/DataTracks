use crate::DefinitionId;
use chrono::Utc;
use serde::Serialize;
use speedy::{Readable, Writable};
#[derive(Clone, Debug, Writable, Readable)]
pub struct InitialMeta {
    pub name: Option<String>,
}

impl InitialMeta {
    pub fn new(name: Option<String>) -> Self {
        InitialMeta { name }
    }
}

#[derive(Clone, Debug, Writable, Readable)]
pub struct TimedMeta {
    pub id: u64,
    pub timestamp: i64,
    pub name: Option<String>,
}

impl TimedMeta {
    pub fn new(id: u64, initial_meta: InitialMeta) -> Self {
        Self {
            id,
            timestamp: Utc::now().timestamp_millis(),
            name: initial_meta.name,
        }
    }
}

#[derive(Clone, Debug, Writable, Readable, Serialize, Default)]
pub struct TargetedMeta {
    pub id: u64,
    pub timestamp: i64,
    pub definition: DefinitionId,
}

impl TargetedMeta {
    pub fn new(meta: TimedMeta, definition: DefinitionId) -> Self {
        Self {
            id: meta.id,
            timestamp: meta.timestamp,
            definition,
        }
    }
}
