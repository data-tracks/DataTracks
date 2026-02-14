use crate::definition::DefinitionFilter::AllMatch;
use crate::mappings::DefinitionMapping;
use crate::{DefinitionId, EntityId, TargetedRecord, TimedMeta, log_channel, Batch};
use flume::{Receiver, Sender, unbounded};
use serde::Serialize;
use speedy::{Readable, Writable};
use std::sync::atomic::{AtomicU64, Ordering};
use value::Value;
use value::Value::Dict;

static ID_BUILDER: AtomicU64 = AtomicU64::new(0);

/// Defines into which final entity an incoming value(primitive to complex) is stored
/// and provides "instructions" on identifying, which parts it is.
#[derive(Clone, Debug, Serialize)]
pub struct Definition {
    pub id: DefinitionId,
    pub name: String,
    filter: DefinitionFilter,
    pub model: Model,
    /// final destination
    pub entity: Entity,
    #[serde(skip)]
    pub native: (Sender<Batch<TargetedRecord>>, Receiver<Batch<TargetedRecord>>),
    pub mapping: DefinitionMapping,
}

impl Definition {
    pub async fn new<S: AsRef<str>>(
        name: S,
        filter: DefinitionFilter,
        mapping: DefinitionMapping,
        model: Model,
        entity: String,
    ) -> Self {
        let id = DefinitionId(ID_BUILDER.fetch_add(1, Ordering::Relaxed));

        let (tx, rx) = unbounded::<Batch<TargetedRecord>>();

        log_channel(
            tx.clone(),
            format!("Definition Native {} - {} to Engine", id.0, name.as_ref()),
            None,
        )
        .await;

        Definition {
            name: name.as_ref().to_string(),
            id,
            filter,
            model,
            entity: Entity::new(entity),
            native: (tx, rx),
            mapping,
        }
    }

    /// does our event match the defined definition
    pub fn matches(&self, value: &Value, meta: &TimedMeta) -> bool {
        match &self.filter {
            AllMatch => true,
            DefinitionFilter::MetaName(n) => meta.name == Some(n.clone()),
            DefinitionFilter::KeyName(k, v) => match value {
                Dict(d) => d.get(k) == Some(&Value::from(v.clone())),
                _ => false,
            },
        }
    }
}

/// incoming values are either accompanied by meta with name or wrapped in a document structure
/// and have a matching value for the key
#[derive(Clone, Debug, Serialize)]
pub enum DefinitionFilter {
    AllMatch,
    MetaName(String),
    KeyName(String, String),
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Debug, Serialize)]
pub enum Model {
    Document,
    Relational,
    Graph,
}

static ENTITY_ID_BUILDER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Writable, Readable, Serialize, Default)]
pub struct Entity {
    pub id: EntityId,
    pub plain: String,
    pub mapped: String,
}

impl Entity {
    pub fn new<S: AsRef<str>>(name: S) -> Self {
        let id = EntityId(ENTITY_ID_BUILDER.fetch_add(1, Ordering::Relaxed));
        Self {
            id,
            plain: name.as_ref().to_string() + "AsIs",
            mapped: name.as_ref().to_string(),
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize)]
pub enum Stage {
    Plain,
    Mapped,
}
