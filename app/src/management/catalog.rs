use engine::EngineKind;
use engine::engine::Engine;
use flume::Sender;
use futures::future::join_all;
use statistics::Event;
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

    pub async fn add_definition(
        &self,
        definition: Definition,
        sender: Sender<Event>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut state = self.state.lock().await;
        for engine in &mut state.engines {
            engine
                .engine_kind
                .create_entity(&definition.entity.plain)
                .await?;
            engine
                .engine_kind
                .create_entity(&definition.entity.non_native)
                .await?;
            engine
                .engine_kind
                .create_entity(&definition.entity.native)
                .await?;

            engine.add_definition(&definition);
        }
        sender
            .send_async(Event::Definition(definition.id, definition.clone()))
            .await?;

        state.definitions.push(definition);
        Ok(())
    }

    pub async fn definitions(&self) -> Vec<Definition> {
        self.state.lock().await.definitions.clone()
    }

    pub async fn engines(&self) -> Vec<Engine> {
        self.state.lock().await.engines.clone()
    }

    pub async fn add_engine(&mut self, engine: EngineKind, sender: Sender<Event>) {
        let engine = Engine::new(engine, sender.clone());
        sender
            .send_async(Event::Engine(engine.id, engine.to_string()))
            .await
            .unwrap();
        self.state.lock().await.engines.push(engine);
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
