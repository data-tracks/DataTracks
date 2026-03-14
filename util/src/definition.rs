use crate::definition::DefinitionFilter::AllMatch;
use crate::mappings::NativeMapping;
use crate::partition::PartitionInfo;
use crate::{Batch, DefinitionId, EntityId, PartitionId, TargetedRecord, TimedMeta, ValueProducer, log_channel};
use flume::{Receiver, Sender, unbounded};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::sync::atomic::{AtomicU64, Ordering};
use processing::Algebra;
use value::Value;
use value::Value::Dict;
use crate::query::Query;

static ID_BUILDER: AtomicU64 = AtomicU64::new(0);

/// Defines into which final entity an incoming value(primitive to complex) is stored
/// and provides "instructions" on identifying, which parts it is.
#[derive(Clone, Debug, Serialize)]
pub struct Definition {
    pub id: DefinitionId,
    pub topic: String,
    filter: DefinitionFilter,
    pub model: Model,
    /// final destination
    pub entity: Entity,
    #[serde(skip)]
    pub native: (
        Sender<Batch<TargetedRecord>>,
        Receiver<Batch<TargetedRecord>>,
    ),
    #[serde(skip)]
    pub process: (
        Sender<Batch<TargetedRecord>>,
        Receiver<Batch<TargetedRecord>>,
    ),
    pub mapping: NativeMapping,
    pub processing: Query,
    pub algebra: Algebra,
    pub partition_info: PartitionInfo,
}

impl Definition {
    pub fn entity_name(&self, id: PartitionId, stage: &Stage) -> String {
        match stage {
            Stage::Plain => {
                format!("{}_{}", self.entity.plain, *id)
            }
            Stage::Native => {
                format!("{}_{}", self.entity.native, *id)
            }
            Stage::Process => {
                format!("{}_{}", self.entity.native, *id)
            }
            _ => "undefined".to_string(),
        }
    }

    pub async fn new<S: AsRef<str>>(
        topic: S,
        filter: DefinitionFilter,
        mapping: NativeMapping,
        processing: Query,
        model: Model,
        entity: String,
    ) -> Self {
        let id = DefinitionId(ID_BUILDER.fetch_add(1, Ordering::Relaxed));

        let (native_tx, native_rx) = unbounded::<Batch<TargetedRecord>>();
        let (process_tx, process_rx) = unbounded::<Batch<TargetedRecord>>();

        log_channel(
            native_tx.clone(),
            format!("Native-{}-{}", id.0, topic.as_ref()),
            None,
        )
        .await;

        log_channel(
            process_tx.clone(),
            format!("Native-{}-{}", id.0, topic.as_ref()),
            None,
        )
        .await;

        Definition {
            topic: topic.as_ref().to_string(),
            id,
            filter,
            model,
            entity: Entity::new(entity),
            native: (native_tx, native_rx),
            process: (process_tx, process_rx),
            mapping,
            processing: processing.clone(),
            algebra: processing.into(),
            partition_info: PartitionInfo::new(),
        }
    }

    pub fn processing(&self) -> ValueProducer {
        Box::new(|v| Some(v.clone()))
    }

    /// does our event match the defined definition
    pub fn matches(&self, value: &Value, meta: &TimedMeta) -> bool {
        match &self.filter {
            AllMatch => true,
            DefinitionFilter::Topic(n) => meta.topics.contains(n),
            DefinitionFilter::KeyName(k, v) => match value {
                Dict(d) => d.get(k) == Some(&Value::from(v.clone())),
                _ => false,
            },
        }
    }
}


/// incoming values are either accompanied by meta with name or wrapped in a document structure
/// and have a matching value for the key
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DefinitionFilter {
    AllMatch,
    #[serde(alias = "topic")]
    Topic(String),
    KeyName(String, String),
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Model {
    #[serde(alias = "document", alias = "DOCUMENT", alias = "DOC", alias = "doc")]
    Document,
    #[serde(
        alias = "relational",
        alias = "RELATIONAL",
        alias = "REL",
        alias = "rel"
    )]
    Relational,
    #[serde(alias = "graph", alias = "GRAPH")]
    Graph,
}

static ENTITY_ID_BUILDER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Writable, Readable, Serialize, Default)]
pub struct Entity {
    pub id: EntityId,
    pub plain: String,
    pub native: String,
    pub process: String,
}

impl Entity {
    pub fn new<S: AsRef<str>>(name: S) -> Self {
        let id = EntityId(ENTITY_ID_BUILDER.fetch_add(1, Ordering::Relaxed));
        Self {
            id,
            plain: name.as_ref().to_string() + "_plain",
            native: name.as_ref().to_string() + "_native",
            process: "".to_string(),
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize)]
pub enum Stage {
    Timer,
    WAL,
    Plain,
    Native,
    Process,
}
