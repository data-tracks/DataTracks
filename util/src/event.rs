use crate::definition::{Definition, Stage};
use crate::{DefinitionId, EngineId};
use serde::Serialize;
use std::collections::HashMap;

pub type DefinitionMeta = (Vec<(DefinitionId, Stage, String, usize)>, Option<String>);

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", content = "data")]
pub enum Event {
    Insert(DefinitionId, usize, EngineId, Stage),
    Definition(DefinitionId, Box<Definition>),
    Engine(EngineId, EngineEvent),
    Runtime(RuntimeEvent),
    EngineStatus(String),
    Queue(QueueEvent),
    Startup(bool),
    Statistics(StatisticEvent),
    HeartBeat(EngineId),
}

#[derive(Serialize, Clone, Debug)]
pub struct RuntimeEvent {
    pub active_tasks: usize,
    pub worker_threads: usize,
    pub blocking_threads: usize,
    pub budget_forces_yield: usize,
}

#[derive(Serialize, Clone, Debug)]
pub struct StatisticEvent {
    pub engines: HashMap<EngineId, DefinitionMeta>,
}

#[derive(Serialize, Clone, Debug)]
pub struct QueueEvent {
    pub name: String,
    pub size: usize,
}

#[derive(Serialize, Clone, Debug)]
pub enum EngineEvent {
    Running(bool),
    Name(String),
}
