use crate::DefinitionId;
use chrono::Utc;
use serde::Serialize;
use smallvec::SmallVec;
use smol_str::SmolStr;
use speedy::{Readable, Writable};
use value::Text;

#[derive(Clone, Debug, Writable, Readable)]
pub struct InitialMeta {
    pub topics: SmallVec<[Text; 4]>,
}

impl InitialMeta {
    pub fn new(topics: Vec<String>) -> Self {
        InitialMeta { topics: SmallVec::from(topics.into_iter().map(|t| Text(SmolStr::new(t)) ).collect::<Vec<_>>())  }
    }
}

#[derive(Clone, Debug, Writable, Readable, Eq, PartialEq)]
pub struct TimedMeta {
    pub id: u64,
    pub timestamp: i64,
    pub topics: SmallVec<[Text; 4]>,
}

impl TimedMeta {
    pub fn new(id: u64, initial_meta: InitialMeta) -> Self {
        Self {
            id,
            timestamp: Utc::now().timestamp_millis(),
            topics: initial_meta.topics,
        }
    }
}

#[derive(Clone, Debug, Writable, Readable, Serialize, Default)]
pub struct TargetedMeta {
    pub id: u64,
    pub timestamp: i64,
    pub definition: DefinitionId,
    pub topics: SmallVec<[Text; 4]>,
}

impl TargetedMeta {
    pub fn new(meta: TimedMeta, definition: DefinitionId) -> Self {
        Self {
            id: meta.id,
            timestamp: meta.timestamp,
            definition,
            topics: meta.topics,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{InitialMeta, TargetedMeta, TimedMeta};

    #[test]
    fn check_sizes() {
        println!("InitialMeta: {} bytes", size_of::<InitialMeta>());
        println!("TimedMeta: {} bytes", size_of::<TimedMeta>());
        println!("TargetedMeta: {} bytes", size_of::<TargetedMeta>());
    }
}
