use crate::connection::PostgresConnection;
use crate::mongo::MongoDB;
use crate::neo::Neo4j;
use crate::postgres::Postgres;
use std::error::Error;
use std::fmt::{Display, Formatter, Write};
use std::ops::{Add, Mul};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;
use util::definition::{Definition, Model};
use util::queue::{RecordContext, RecordQueue};
use value::Value;

#[derive(Clone)]
pub struct Engine {
    pub queue: RecordQueue,
    pub engine_kind: EngineKind,
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
    pub fn new(engine_kind: EngineKind) -> Self {
        Engine {
            queue: RecordQueue::new(),
            engine_kind,
        }
    }

    pub async fn stop(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &self.engine_kind {
            EngineKind::Postgres(p) => p.stop().await,
            EngineKind::MongoDB(m) => m.stop().await,
            EngineKind::Neo4j(n) => n.stop().await,
        }
    }

    pub async fn next(
        &self,
        value: Value,
        context: RecordContext,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.queue.push(value, context).await
    }

    /// Mixture between current running tx, complexity of mapping (and user suggestion).
    pub fn cost(&self, value: &Value, definition: &Definition) -> f64 {
        let cost = match &self.engine_kind {
            EngineKind::Postgres(p) => p.cost(value),
            EngineKind::MongoDB(m) => m.cost(value),
            EngineKind::Neo4j(n) => n.cost(value),
        };

        let mut cost = cost.mul(0.01.mul(self.queue.len().add(1) as f64));

        if definition.model != self.model() {
            cost *= 2.0;
        }

        cost *= self.current_load();

        cost
    }

    fn current_load(&self) -> f64 {
        match &self.engine_kind {
            EngineKind::Postgres(p) => p.current_load(),
            EngineKind::MongoDB(m) => m.current_load(),
            EngineKind::Neo4j(n) => n.current_load(),
        }
        .to_f64()
    }

    fn model(&self) -> Model {
        match self.engine_kind {
            EngineKind::Postgres(_) => Model::Relational,
            EngineKind::MongoDB(_) => Model::Document,
            EngineKind::Neo4j(_) => Model::Graph,
        }
    }

    pub async fn store(
        &self,
        value: Value,
        context: RecordContext,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let entity = context.entity.unwrap_or(String::from("_stream"));
        match &self.engine_kind {
            EngineKind::Postgres(p) => p.store(value, entity).await,
            EngineKind::MongoDB(m) => m.store(value, entity).await,
            EngineKind::Neo4j(n) => n.store(value, entity).await,
        }
    }
}

#[derive(Clone)]
pub enum EngineKind {
    Postgres(Postgres),
    MongoDB(MongoDB),
    Neo4j(Neo4j),
}

impl Display for EngineKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineKind::Postgres(_) => f.write_str("postgres"),
            EngineKind::MongoDB(_) => f.write_str("mongodb"),
            EngineKind::Neo4j(_) => f.write_str("neo4j"),
        }
    }
}

impl EngineKind {
    pub async fn create_entity(&self, name: &String) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(match self {
            EngineKind::Postgres(p) => p.create_table(name).await?,
            EngineKind::MongoDB(m) => m.create_collection(name).await?,
            EngineKind::Neo4j(_) => {}
        })
    }

    pub async fn start_all() -> Result<Vec<EngineKind>, Box<dyn Error + Send + Sync>> {
        let mut engines = vec![];

        let mut pg = EngineKind::postgres();
        pg.start().await?;
        pg.monitor().await?;

        let mut mongodb = EngineKind::mongo_db();
        mongodb.start().await?;
        mongodb.monitor().await?;

        let mut neo4j = EngineKind::neo4j();
        neo4j.start().await?;
        neo4j.monitor().await?;

        engines.push(pg.into());
        engines.push(mongodb.into());
        engines.push(neo4j.into());

        Ok(engines)
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            EngineKind::Postgres(p) => p.start().await,
            EngineKind::MongoDB(m) => m.start().await,
            EngineKind::Neo4j(n) => n.start().await,
        }
    }

    pub async fn stop(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            EngineKind::Postgres(p) => p.stop().await,
            EngineKind::MongoDB(m) => m.stop().await,
            EngineKind::Neo4j(n) => n.stop().await,
        }
    }

    pub async fn monitor(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let engine = self.clone();

        spawn(async move {
            loop {
                match &engine {
                    EngineKind::Postgres(p) => p.monitor().await.unwrap(),
                    EngineKind::MongoDB(m) => m.monitor().await.unwrap(),
                    EngineKind::Neo4j(n) => n.monitor().await.unwrap(),
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
        }
    }

    fn mongo_db() -> MongoDB {
        MongoDB {
            load: Arc::new(Mutex::new(Load::Low)),
            client: None,
        }
    }

    fn neo4j() -> Neo4j {
        Neo4j {
            load: Arc::new(Mutex::new(Load::Low)),
            host: "localhost".to_string(),
            port: 7687,
            user: "neo4j".to_string(),
            password: "neoneoneo".to_string(),
            database: "neo4j".to_string(),
            graph: None,
        }
    }
}

#[derive(Clone)]
pub enum Load {
    Low,
    Middle,
    High,
}

impl Load {
    fn to_f64(&self) -> f64 {
        match self {
            Load::Low => 1.0,
            Load::Middle => 2.0,
            Load::High => 5.0,
        }
    }
}
