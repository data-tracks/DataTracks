use std::sync::{Arc, Mutex};

use crate::management::storage::Storage;

pub fn start() -> Arc<Mutex<Storage>> {
    Arc::new(Mutex::new(Storage::new()))
}