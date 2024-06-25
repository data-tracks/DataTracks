use std::sync::Arc;

use crate::mangagement::storage::Storage;

pub fn start() -> Arc<Storage> {
    Arc::new(Storage::new())
}