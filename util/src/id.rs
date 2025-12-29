use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(
    Copy, Clone, Debug, Serialize, Deserialize, Readable, Writable, Hash, Eq, PartialEq, Default,
)]
pub struct EngineId(pub u64);
#[derive(
    Copy, Clone, Debug, Serialize, Deserialize, Readable, Writable, Hash, Eq, PartialEq, Default,
)]
pub struct EntityId(pub u64);

#[derive(
    Copy, Clone, Debug, Serialize, Deserialize, Readable, Writable, Hash, Eq, PartialEq, Default,
)]
pub struct DefinitionId(pub u64);

static GLOBAL_ID: AtomicUsize = AtomicUsize::new(0);

pub fn new_id() -> usize {
    GLOBAL_ID.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use crate::id::new_id;

    #[test]
    fn not_same() {
        let mut ids = vec![];

        for _ in 0..1000 {
            let id = new_id();
            if ids.contains(&(id)) {
                panic!("overlapping ids")
            }
            ids.push(id)
        }
    }
}
