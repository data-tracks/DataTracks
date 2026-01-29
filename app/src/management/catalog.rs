use engine::engine::Engine;
use flume::Sender;
use futures::future::join_all;
use std::sync::Arc;
use tokio::sync::Mutex;
use util::definition::Definition;
use util::{EngineEvent, Event};

pub struct Catalog {
    state: Arc<Mutex<State>>,
    statistic_tx: Sender<Event>,
}

#[derive(Default)]
pub struct State {
    definitions: Vec<Definition>,
    engines: Vec<Engine>,
}

impl Catalog {
    pub fn new(statistic_tx: Sender<Event>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            statistic_tx,
        }
    }

    pub async fn add_definition(
        &self,
        definition: Definition,
        sender: Sender<Event>,
    ) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        for engine in &mut state.engines {
            engine.engine_kind.init_entity(&definition).await?;

            engine.add_definition(&definition);
        }
        sender
            .send_async(Event::Definition(
                definition.id,
                Box::new(definition.clone()),
            ))
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

    pub async fn add_engine(&mut self, engine: Engine) {
        let id = engine.id;
        let name = engine.to_string();
        self.state.lock().await.engines.push(engine);
        self.statistic_tx
            .send_async(Event::Engine(id, EngineEvent::Name(name)))
            .await
            .unwrap();
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
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
            statistic_tx: self.statistic_tx.clone(),
        }
    }
}
