use engine::EngineKind;
use engine::engine::Engine;
use futures::future::join_all;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use util::definition::Definition;
use util::queue::RecordQueue;

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

    pub async fn add_definition(
        &self,
        definition: Definition,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut state = self.state.lock().await;
        for engine in &state.engines {
            engine.engine_kind.create_entity(&definition.entity).await?;
        }

        state.definitions.push(definition);
        Ok(())
    }

    pub async fn definitions(&self) -> Vec<Definition> {
        self.state.lock().await.definitions.clone()
    }

    pub async fn engines(&self) -> Vec<Engine> {
        self.state.lock().await.engines.clone()
    }

    pub async fn add_engine(&mut self, engine: EngineKind) {
        self.state.lock().await.engines.push(Engine::new(engine))
    }

    pub async fn stop(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let futures: Vec<_> = self
            .state
            .lock()
            .await
            .engines
            .clone()
            .into_iter()
            .map(|engine| engine.stop())
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
