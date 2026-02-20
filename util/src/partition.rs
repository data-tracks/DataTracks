use dashmap::DashMap;
use serde::Serialize;
use serde_with::serde_as;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[serde_as]
#[derive(Clone, Debug, Serialize)]
pub struct PartitionInfo {
    #[serde_as(as = "Arc<_>")]
    state: Arc<State>,
}

impl Default for PartitionInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionInfo {
    pub fn new() -> Self {
        Self {
            state: Arc::new(State {
                partitions: Default::default(),
                closed: vec![],
                next: Default::default(),
            }),
        }
    }

    pub fn next(&self, worker_id: &WorkerId, size: &u64) -> u64 {
        // 1. Get or Create the partition.
        // DashMap handles the internal locking for this specific key.
        let mut entry = self.state.partitions.entry(*worker_id).or_insert_with(|| {
            let id = self.state.next.fetch_add(1, Ordering::Relaxed);
            Partition {
                partition_id: id.into(),
                size: 0,
            }
        });

        let partition = entry.value_mut();

        // 2. Logic Check: Does the new size exceed the limit?
        if partition.size + size > 1_000_000 {
            // Rotate: Fetch new global ID
            let new_id = self.state.next.fetch_add(1, Ordering::Relaxed);
            partition.partition_id = new_id.into();
            partition.size = *size;
            new_id
        } else {
            // Increment: Update in-place
            partition.size += size;
            partition.partition_id.0
        }
    }
}

#[derive(Debug, Serialize)]
struct State {
    partitions: DashMap<WorkerId, Partition>,
    closed: Vec<u64>,
    next: AtomicU64,
}

#[derive(Copy, Clone, Debug, Serialize, Eq, Hash, PartialEq)]
pub struct PartitionId(pub u64);

impl From<u64> for PartitionId {
    fn from(value: u64) -> Self {
        PartitionId(value)
    }
}

impl Deref for PartitionId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Copy, Clone, Debug, Serialize, Eq, Hash, PartialEq)]
pub struct WorkerId(u64);

impl From<u64> for WorkerId {
    fn from(value: u64) -> Self {
        WorkerId(value)
    }
}

#[derive(Copy, Clone, Debug, Serialize)]
struct Partition {
    partition_id: PartitionId,
    size: u64,
}
