use crate::management::definition::Definition;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct Catalog {
    definitions: Arc<Mutex<Vec<Definition>>>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            definitions: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Clone for Catalog {
    fn clone(&self) -> Self {
        Catalog {
            definitions: self.definitions.clone(),
        }
    }
}
