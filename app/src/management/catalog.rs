use util::definition::Definition;
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

    pub async fn add_definition(&self, definition: Definition) {
        self.definitions.lock().await.push(definition)
    }

    pub async fn definitions(&self) -> Vec<Definition> {
        self.definitions.lock().await.clone()
    }
}

impl Clone for Catalog {
    fn clone(&self) -> Self {
        Catalog {
            definitions: self.definitions.clone(),
        }
    }
}
