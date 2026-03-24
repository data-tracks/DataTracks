use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};

pub trait Identifiable {
    fn id(&self) -> u64;
}

#[derive(
    Copy, Clone, Debug, Serialize, Deserialize, Readable, Writable, Hash, Eq, PartialEq, Default,
)]
pub struct EngineId(pub u64);

impl Display for EngineId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for EngineId {
    fn from(value: u64) -> Self {
        EngineId(value)
    }
}

impl Deref for EngineId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for EngineId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(
    Copy, Clone, Debug, Serialize, Deserialize, Readable, Writable, Hash, Eq, PartialEq, Default,
)]
pub struct EntityId(pub u64);

impl Deref for EntityId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for EntityId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(
    Copy, Clone, Debug, Serialize, Deserialize, Readable, Writable, Hash, Eq, PartialEq, Default,
)]
pub struct DefinitionId(pub u64);

impl Deref for DefinitionId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DefinitionId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

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
