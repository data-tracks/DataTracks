use engine::Engine;
use futures::future::join_all;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use util::definition::Definition;

#[derive(Default)]
pub struct Catalog {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
pub struct State {
    definitions: Vec<Definition>,
    engines: Vec<Engine>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    pub async fn add_definition(&self, definition: Definition) {
        self.state.lock().await.definitions.push(definition)
    }

    pub async fn definitions(&self) -> Vec<Definition> {
        self.state.lock().await.definitions.clone()
    }

    pub async fn engines(&self) -> Vec<Engine> {
        self.state.lock().await.engines.clone()
    }

    pub async fn add_engine(&mut self, engine: Engine) {
        self.state.lock().await.engines.push(engine)
    }

    pub async fn stop(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let futures: Vec<_> = self
            .state
            .lock()
            .await
            .engines
            .clone()
            .into_iter()
            .map(|mut engine| engine.stop())
            .collect();
        join_all(futures).await.into_iter().collect()
    }
}

impl Clone for Catalog {
    fn clone(&self) -> Self {
        Catalog {
            state: self.state.clone(),
        }
    }
}
