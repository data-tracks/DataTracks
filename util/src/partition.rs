use serde::Serialize;
use serde_with::serde_as;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

#[serde_as]
#[derive(Clone, Debug, Serialize)]
pub struct PartitionInfo {
    #[serde_as(as = "Arc<Mutex<_>>")]
    state: Arc<Mutex<State>>,
}

impl PartitionInfo {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                partitions: Default::default(),
                closed: vec![],
                next: Default::default(),
            })),
        }
    }

    pub fn next(&self, worker_id: u64, size: u64) -> u64 {
        let mut state = self.state.lock().unwrap();
        let current_size = state.partitions.entry(worker_id).or_default();
        let new_size = *current_size + size;
        if new_size > 1_000_000 {
        } else {
            state.partitions.entry(worker_id).or_insert(new_size);
        }
        0
    }
}

#[derive(Debug, Serialize)]
struct State {
    partitions: HashMap<u64, u64>,
    closed: Vec<u64>,
    next: AtomicU64,
}
