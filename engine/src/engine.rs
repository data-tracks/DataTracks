use crate::connection::PostgresConnection;
use crate::mongo::MongoDB;
use crate::neo::Neo4j;
use crate::postgres::Postgres;
use derive_more::From;
use flume::{Receiver, Sender, unbounded};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Mul;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::sleep;
use util::definition::{Definition, Model, Stage};
use util::{Batch, DefinitionId, EngineId, Event, TargetedRecord, log_channel};
use value::Value;

static ID_BUILDER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct Engine {
    pub tx: Sender<TargetedRecord>,
    pub rx: Receiver<TargetedRecord>,
    pub ids: Vec<u64>,
    pub statistic_sender: Sender<Event>,
    pub engine_kind: EngineKind,
    pub id: EngineId,
    pub definitions: HashMap<DefinitionId, Definition>,
}

impl AsRef<EngineKind> for Engine {
    fn as_ref(&self) -> &EngineKind {
        &self.engine_kind
    }
}

impl Display for Engine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{}", self.engine_kind).as_str())
    }
}

impl Engine {
    pub async fn new(engine_kind: EngineKind, sender: Sender<Event>) -> Self {
        let (tx, rx) = unbounded::<TargetedRecord>();

        let name = format!("Persister {}", engine_kind);
        log_channel(tx.clone(), name, None).await;

        let id = EngineId(ID_BUILDER.fetch_add(1, Ordering::Relaxed));
        Engine {
            id,
            tx,
            rx,
            ids: vec![],
            statistic_sender: sender,
            engine_kind,
            definitions: Default::default(),
        }
    }

    pub async fn stop(self) -> anyhow::Result<()> {
        match &self.engine_kind {
            EngineKind::Postgres(p) => p.stop().await,
            EngineKind::MongoDB(m) => m.stop().await,
            EngineKind::Neo4j(n) => n.stop().await,
        }
    }

    pub fn add_definition(&mut self, definition: &Definition) {
        self.definitions.insert(definition.id, definition.clone());
    }

    /// Mixture between current running tx, complexity of mapping (and user suggestion).
    pub fn cost(&self, value: &Value, definition: &Definition) -> f64 {
        let cost = match &self.engine_kind {
            EngineKind::Postgres(p) => p.cost(value),
            EngineKind::MongoDB(m) => m.cost(value),
            EngineKind::Neo4j(n) => n.cost(value),
        };

        let pressure = self.rx.len() + 1;

        let mut cost = cost.mul(pressure as f64);

        if definition.model != self.model() {
            cost *= 2.0;
        }

        cost
    }

    pub fn model(&self) -> Model {
        match self.engine_kind {
            EngineKind::Postgres(_) => Model::Relational,
            EngineKind::MongoDB(_) => Model::Document,
            EngineKind::Neo4j(_) => Model::Graph,
        }
    }

    pub async fn store(
        &mut self,
        stage: Stage,
        entity_name: String,
        values: &Batch<TargetedRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let ids = values
            .iter()
            .map(|TargetedRecord { value: _, meta }| meta.id)
            .collect::<Vec<_>>();
        self.ids.extend(ids.clone());

        match &self.engine_kind {
            EngineKind::Postgres(p) => p.store(stage, entity_name, values).await,
            EngineKind::MongoDB(m) => m.store(stage, entity_name, values).await,
            EngineKind::Neo4j(n) => n.store(stage, entity_name, values).await,
        }
    }

    pub async fn read(
        &mut self,
        entity: String,
        ids: Vec<u64>,
    ) -> Result<Vec<Value>, Box<dyn Error + Send + Sync>> {
        match &self.engine_kind {
            EngineKind::Postgres(p) => p.read(entity, ids).await,
            EngineKind::MongoDB(m) => m.read(entity, ids).await,
            EngineKind::Neo4j(n) => n.read(entity, ids).await,
        }
    }
}

#[derive(Clone, From, Debug)]
pub enum EngineKind {
    Postgres(Postgres),
    MongoDB(MongoDB),
    Neo4j(Neo4j),
}

impl Display for EngineKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineKind::Postgres(_) => f.write_str("postgres"),
            EngineKind::MongoDB(_) => f.write_str("mongodb"),
            EngineKind::Neo4j(_) => f.write_str("neo4j"),
        }
    }
}

impl EngineKind {
    pub async fn init_entity(&mut self, definition: &Definition) -> anyhow::Result<()> {
        match self {
            EngineKind::Postgres(p) => p.init_entity(definition).await?,
            EngineKind::MongoDB(m) => m.init_entity(definition).await?,
            EngineKind::Neo4j(n) => n.init_entity(definition).await,
        };
        Ok(())
    }

    pub async fn start(
        &mut self,
        join: &mut JoinSet<()>,
        sender: Sender<Event>,
    ) -> anyhow::Result<()> {
        match self {
            EngineKind::Postgres(p) => p.start(join).await?,
            EngineKind::MongoDB(m) => m.start().await?,
            EngineKind::Neo4j(n) => n.start().await?,
        }
        self.monitor(join, sender.clone()).await
    }

    pub async fn start_all(
        join: &mut JoinSet<()>,
        statistic_tx: Sender<Event>,
    ) -> anyhow::Result<Vec<Engine>> {
        let engine_kinds: Vec<EngineKind> = vec![
            EngineKind::postgres().into(),
            EngineKind::mongo_db().into(),
            EngineKind::neo4j().into(),
        ];

        let mut engines: Vec<Engine> = vec![];

        for mut engine in &mut engine_kinds.into_iter() {
            engine.start(join, statistic_tx.clone()).await?;
            engines.push(Engine::new(engine, statistic_tx.clone()).await);
        }

        Ok(engines)
    }

    pub async fn stop(self) -> anyhow::Result<()> {
        match self {
            EngineKind::Postgres(p) => p.stop().await,
            EngineKind::MongoDB(m) => m.stop().await,
            EngineKind::Neo4j(n) => n.stop().await,
        }
    }

    pub async fn monitor(
        &mut self,
        join: &mut JoinSet<()>,
        statistic_tx: Sender<Event>,
    ) -> anyhow::Result<()> {
        let engine = self.clone();

        join.spawn(async move {
            loop {
                match &engine {
                    EngineKind::Postgres(p) => p.monitor(&statistic_tx).await.unwrap(),
                    EngineKind::MongoDB(m) => m.monitor(&statistic_tx).await.unwrap(),
                    EngineKind::Neo4j(n) => n.monitor(&statistic_tx).await.unwrap(),
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
        Ok(())
    }

    pub fn postgres() -> Postgres {
        Self::postgres_with_port(5432)
    }

    pub fn postgres_with_port(port: u16) -> Postgres {
        Postgres {
            name: "engine-postgres".to_string(),
            load: Arc::new(Mutex::new(Load::Low)),
            connector: PostgresConnection {
                url: "localhost".to_string(),
                port,
                db: "postgres".to_string(),
                user: "postgres".to_string(),
                password: "postgres".to_string(),
            },
            client: None,
            prepared_statements: Default::default(),
        }
    }

    fn mongo_db() -> MongoDB {
        MongoDB {
            load: Arc::new(Mutex::new(Load::Low)),
            client: None,
            names: Default::default(),
        }
    }

    pub(crate) fn neo4j() -> Neo4j {
        Self::neo4j_with_port(7687)
    }

    pub(crate) fn neo4j_with_port(port: u16) -> Neo4j {
        Neo4j {
            name: "neo4j-engine".to_string(),
            load: Arc::new(Mutex::new(Load::Low)),
            host: "localhost".to_string(),
            port,
            user: "neo4j".to_string(),
            password: "neoneoneo".to_string(),
            graph: None,
            prepared_queries: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Load {
    Low,
    Middle,
    High,
}
