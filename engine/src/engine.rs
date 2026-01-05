use crate::connection::PostgresConnection;
use crate::mongo::MongoDB;
use crate::neo::Neo4j;
use crate::postgres::Postgres;
use derive_more::From;
use flume::{Receiver, Sender, unbounded};
use statistics::Event;
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
use util::{DefinitionId, EngineId, TargetedMeta, log_channel};
use value::Value;

static ID_BUILDER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct Engine {
    pub tx: Sender<(Value, TargetedMeta)>,
    pub rx: Receiver<(Value, TargetedMeta)>,
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
    pub fn new(engine_kind: EngineKind, sender: Sender<Event>) -> Self {
        let (tx, rx) = unbounded::<(Value, TargetedMeta)>();
        log_channel(tx.clone(), engine_kind.to_string());

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

    pub async fn stop(self) -> Result<(), Box<dyn Error + Send + Sync>> {
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
        values: Vec<(Value, TargetedMeta)>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let ids = values.iter().map(|(_v, m)| m.id).collect::<Vec<_>>();
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
    pub async fn init_entity(
        &mut self,
        definition: &Definition,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            EngineKind::Postgres(p) => p.init_entity(definition).await?,
            EngineKind::MongoDB(m) => m.init_entity(definition).await?,
            EngineKind::Neo4j(n) => n.init_entity(definition).await,
        };
        Ok(())
    }

    pub async fn start_all(
        join: &mut JoinSet<()>,
        statistic_tx: Sender<Event>,
    ) -> Result<Vec<EngineKind>, Box<dyn Error + Send + Sync>> {
        let mut engines = vec![];

        let mut pg = EngineKind::postgres();
        pg.start(join).await?;
        let mut pg = EngineKind::from(pg);
        EngineKind::monitor(&mut pg, join, statistic_tx.clone()).await?;

        let mut mongodb = EngineKind::mongo_db();
        mongodb.start().await?;
        let mut mongodb = EngineKind::from(mongodb);
        EngineKind::monitor(&mut mongodb, join, statistic_tx.clone()).await?;

        let mut neo4j = EngineKind::neo4j();
        neo4j.start().await?;
        let mut neo4j = EngineKind::from(neo4j);
        EngineKind::monitor(&mut neo4j, join, statistic_tx).await?;

        engines.push(pg);
        engines.push(mongodb);
        engines.push(neo4j);

        Ok(engines)
    }

    pub async fn stop(self) -> Result<(), Box<dyn Error + Send + Sync>> {
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
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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
        Postgres {
            load: Arc::new(Mutex::new(Load::Low)),
            connector: PostgresConnection {
                url: "localhost".to_string(),
                port: 5432,
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
        Neo4j {
            load: Arc::new(Mutex::new(Load::Low)),
            host: "localhost".to_string(),
            port: 7687,
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
