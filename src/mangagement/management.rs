use std::sync::{Arc, Mutex};

use crate::mangagement::storage::Storage;

pub fn start() -> Arc<Mutex<Storage>> {
    Arc::new(Mutex::new(Storage::new()))
}