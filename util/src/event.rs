use crate::definition::{Definition, Stage};
use crate::{DefinitionId, EngineId};
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;

pub type DefinitionMeta = (Vec<(DefinitionId, Stage, String, u64)>, Option<String>);

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", content = "data")]
pub enum Event {
    Insert {
        id: DefinitionId,
        size: u64,
        ids: Vec<u64>,
        source: EngineId,
        stage: Stage,
    },
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

#[derive(Serialize, Clone, Debug, Default)]
pub struct ThroughputEvent {
    pub tps: HashMap<String, ThroughputMeta>,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct StatisticEvent {
    pub engines: HashMap<EngineId, DefinitionMeta>,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct ThroughputMeta {
    plain: f64,
    mapped: f64,
}

impl StatisticEvent {
    pub fn calculate(
        &self,
        previous: StatisticEvent,
        since: Duration,
    ) -> HashMap<String, ThroughputMeta> {
        let current_amounts = self.get_amounts();
        let last_amounts = previous.get_amounts();

        let mut tps = HashMap::new();

        for (id, amounts) in current_amounts {
            let mut tp = ThroughputMeta::default();
            if let Some(amounts_last) = last_amounts.get(&id) {
                let raw_tps = (amounts.plain - amounts_last.plain) / since.as_secs() as f64;
                // Apply rounding to 3 decimal places
                let rounded_tps = (raw_tps * 1000.0).round() / 1000.0;

                tp.plain = rounded_tps;

                let raw_tps = (amounts.mapped - amounts_last.mapped) / since.as_secs() as f64;
                // Apply rounding to 3 decimal places
                let rounded_tps = (raw_tps * 1000.0).round() / 1000.0;

                tp.mapped = rounded_tps;
            }
            tps.insert(id, tp);
        }

        tps
    }

    pub fn get_amounts(&self) -> HashMap<String, ThroughputMeta> {
        let mut map = HashMap::new();

        for def in self.engines.values() {
            let name = def.1.clone().unwrap_or("unnamed".to_string());
            let mut tp = ThroughputMeta::default();
            for (_, stage, _, amount) in &def.0 {
                match stage {
                    Stage::Plain => {
                        tp.plain = *amount as f64;
                    }
                    Stage::Mapped => {
                        tp.mapped = *amount as f64;
                    }
                }
            }
            map.insert(name, tp);
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
