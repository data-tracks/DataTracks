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
    Throughput(ThroughputEvent),
    Heartbeat(String),
}

#[derive(Serialize, Clone, Debug)]
pub struct RuntimeEvent {
    pub active_tasks: usize,
    pub worker_threads: usize,
    pub blocking_threads: usize,
    pub budget_forces_yield: usize,
}

#[derive(Serialize, Clone, Debug)]
pub struct ThroughputEvent {
    pub tps: HashMap<String, f64>,
}

#[derive(Serialize, Clone, Debug)]
pub struct StatisticEvent {
    pub engines: HashMap<EngineId, DefinitionMeta>,
}

impl StatisticEvent {
    pub fn get_amounts(&self) -> HashMap<String, usize> {
        let mut map = HashMap::new();

        for def in self.engines.values() {
            map.insert(def.1.clone().unwrap_or("unnamed".to_string()), def.0.iter().map(|v|v.3).sum());
        }

        map
    }
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
