use serde::Serialize;
use crate::{DefinitionId, EngineId};
use crate::definition::Definition;

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type",content = "data")]
pub enum Event {
    Insert(DefinitionId, usize, EngineId),
    Definition(DefinitionId, Definition),
    Engine(EngineId, String),
    Runtime(RuntimeEvent),
    EngineStatus(String),
    Queue(QueueEvent),
}

#[derive(Serialize, Clone, Debug)]
pub struct RuntimeEvent {
    pub active_tasks: usize,
    pub worker_threads: usize,
    pub blocking_threads: usize,
    pub budget_forces_yield: usize,
}

#[derive(Serialize, Clone, Debug)]
pub struct QueueEvent {
    pub name: String,
    pub size: usize,
}