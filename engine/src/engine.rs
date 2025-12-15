use crate::connection::PostgresConnection;
use crate::mongo::MongoDB;
use crate::neo::Neo4j;
use crate::postgres::Postgres;
use std::error::Error;
use std::fmt::{Display, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::spawn;
use tokio::task::JoinSet;
use tokio::time::sleep;
use util::definition::{Definition, Model};
use util::queue::{Meta, RecordQueue};
use value::Value;

#[derive(Clone)]
pub enum Engine {
    Postgres(Postgres),
    MongoDB(MongoDB),
    Neo4j(Neo4j),
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Postgres(_) => f.write_str("postgres"),
            Engine::MongoDB(_) => f.write_str("mongodb"),
            Engine::Neo4j(_) => f.write_str("neo4j"),
        }
    }
}

impl Engine {
    pub async fn store(&self, value: Value) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            Engine::Postgres(p) => p.store(value).await,
            Engine::MongoDB(m) => m.store(value).await,
            Engine::Neo4j(n) => n.store(value).await,
        }
    }

    pub async fn next(&self, meta: Meta, value: Value) -> Result<(), Box<dyn Error + Send + Sync>> {
        let res = match self {
            Engine::Postgres(p) => p.queue.push(meta, value).await,
            Engine::MongoDB(m) => m.queue.push(meta, value).await,
            Engine::Neo4j(n) => n.queue.push(meta, value).await,
        };
        Ok(())
    }

    pub async fn start_all(
        set: &mut JoinSet<()>,
    ) -> Result<Vec<(RecordQueue, Engine)>, Box<dyn Error + Send + Sync>> {
        let mut engines: Vec<(RecordQueue, Engine)> = vec![];

        let post_queue = RecordQueue::new();
        let mut pg = Engine::postgres(post_queue.clone());
        pg.start().await?;
        pg.monitor().await?;
        pg.create_tables().await?;
        for _ in 0..1_000 {
            pg.insert_data().await?;
        }

        let mongo_queue = RecordQueue::new();
        let mut mongodb = Engine::mongo_db(mongo_queue.clone());
        mongodb.start().await?;
        mongodb.monitor().await?;
        mongodb.create_collection().await?;
        for _ in 0..1_000 {
            mongodb.insert_data().await?;
        }

        let neo_queue = RecordQueue::new();
        let mut neo4j = Engine::neo4j(neo_queue.clone());
        neo4j.start().await?;
        neo4j.monitor().await?;
        for _ in 0..1_000 {
            neo4j.insert_data().await?;
        }

        engines.push((post_queue, pg.into()));
        engines.push((mongo_queue, mongodb.into()));
        engines.push((neo_queue, neo4j.into()));

        for (queue, engine) in &engines {
            let mut clone = engine.clone();
            let mut queue = queue.clone();
            set.spawn(async move {
                loop {
                    match queue.pop() {
                        None => sleep(Duration::from_millis(1)).await,
                        Some((_, v)) => clone.store(v).await.unwrap(),
                    }
                }
            });
        }

        Ok(engines)
    }

    /// Mixture between current running tx, complexity of mapping (and user suggestion).
    pub fn cost(&self, value: &Value, definition: &Definition) -> f64 {
        let mut cost = match self {
            Engine::Postgres(p) => p.cost(value),
            Engine::MongoDB(m) => m.cost(value),
            Engine::Neo4j(n) => n.cost(value),
        };

        if definition.model != self.model() {
            cost *= 2.0;
        }

        cost *= self.current_load();

        cost
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            Engine::Postgres(p) => p.start().await,
            Engine::MongoDB(m) => m.start().await,
            Engine::Neo4j(n) => n.start().await,
        }
    }

    pub async fn stop(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            Engine::Postgres(mut p) => p.stop().await,
            Engine::MongoDB(mut m) => m.stop().await,
            Engine::Neo4j(mut n) => n.stop().await,
        }
    }

    pub async fn monitor(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let engine = self.clone();

        spawn(async move {
            loop {
                match &engine {
                    Engine::Postgres(p) => p.monitor().await.unwrap(),
                    Engine::MongoDB(m) => m.monitor().await.unwrap(),
                    Engine::Neo4j(n) => n.monitor().await.unwrap(),
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
        Ok(())
    }

    pub fn postgres(queue: RecordQueue) -> Postgres {
        Postgres {
            queue,
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

    fn mongo_db(queue: RecordQueue) -> MongoDB {
        MongoDB {
            queue,
            load: Arc::new(Mutex::new(Load::Low)),
            client: None,
        }
    }

    fn neo4j(queue: RecordQueue) -> Neo4j {
        Neo4j {
            queue,
            load: Arc::new(Mutex::new(Load::Low)),
            host: "localhost".to_string(),
            port: 7687,
            user: "neo4j".to_string(),
            password: "neoneoneo".to_string(),
            database: "neo4j".to_string(),
            graph: None,
        }
    }

    fn model(&self) -> Model {
        match self {
            Engine::Postgres(_) => Model::Relational,
            Engine::MongoDB(_) => Model::Document,
            Engine::Neo4j(_) => Model::Graph,
        }
    }

    fn current_load(&self) -> f64 {
        match self {
            Engine::Postgres(p) => p.current_load(),
            Engine::MongoDB(m) => m.current_load(),
            Engine::Neo4j(n) => n.current_load(),
        }
        .to_f64()
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
