use crate::connection::PostgresConnection;
use crate::mongo::MongoDB;
use crate::neo::Neo4j;
use crate::postgres::Postgres;
use std::error::Error;
use std::fmt::{Display, Write};
use std::ops::{Add, Mul};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;
use util::definition::{Definition, Model};
use util::queue::{RecordContext, RecordQueue};
use value::Value;

#[derive(Clone)]
pub enum Engine {
    Postgres(Postgres, RecordQueue),
    MongoDB(MongoDB, RecordQueue),
    Neo4j(Neo4j, RecordQueue),
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Postgres(_, _) => f.write_str("postgres"),
            Engine::MongoDB(_, _) => f.write_str("mongodb"),
            Engine::Neo4j(_, _) => f.write_str("neo4j"),
        }
    }
}

impl Engine {
    pub async fn store(
        &self,
        value: Value,
        context: RecordContext,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let entity = context.entity.unwrap_or(String::from("_stream"));
        match self {
            Engine::Postgres(p, _) => p.store(value, entity).await,
            Engine::MongoDB(m, _) => m.store(value, entity).await,
            Engine::Neo4j(n, _) => n.store(value, entity).await,
        }
    }

    pub async fn next(
        &self,
        value: Value,
        context: RecordContext,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let res = match self {
            Engine::Postgres(p, _) => p.queue.push(value, context).await,
            Engine::MongoDB(m, _) => m.queue.push(value, context).await,
            Engine::Neo4j(n, _) => n.queue.push(value, context).await,
        };
        Ok(())
    }

    pub async fn create_entity(&self, name: &String) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(match self {
            Engine::Postgres(p, _) => p.create_table(name).await?,
            Engine::MongoDB(m, _) => m.create_collection(name).await?,
            Engine::Neo4j(_, _) => {}
        })
    }

    pub async fn start_all() -> Result<Vec<(RecordQueue, Engine)>, Box<dyn Error + Send + Sync>> {
        let mut engines: Vec<(RecordQueue, Engine)> = vec![];

        let post_queue = RecordQueue::new();
        let mut pg = Engine::postgres(post_queue.clone());
        pg.start().await?;
        pg.monitor().await?;

        let mongo_queue = RecordQueue::new();
        let mut mongodb = Engine::mongo_db(mongo_queue.clone());
        mongodb.start().await?;
        mongodb.monitor().await?;

        let neo_queue = RecordQueue::new();
        let mut neo4j = Engine::neo4j(neo_queue.clone());
        neo4j.start().await?;
        neo4j.monitor().await?;

        engines.push((post_queue, pg.into()));
        engines.push((mongo_queue, mongodb.into()));
        engines.push((neo_queue, neo4j.into()));

        Ok(engines)
    }

    /// Mixture between current running tx, complexity of mapping (and user suggestion).
    pub fn cost(&self, value: &Value, definition: &Definition) -> f64 {
        let mut cost = match self {
            Engine::Postgres(p, queue) => p.cost(value).mul(0.01.mul(queue.len().add(1) as f64)),
            Engine::MongoDB(m, queue) => m.cost(value).mul(0.01.mul(queue.len().add(1) as f64)),
            Engine::Neo4j(n, queue) => n.cost(value).mul(0.01.mul(queue.len().add(1) as f64)),
        };

        if definition.model != self.model() {
            cost *= 2.0;
        }

        cost *= self.current_load();

        cost
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            Engine::Postgres(p, _) => p.start().await,
            Engine::MongoDB(m, _) => m.start().await,
            Engine::Neo4j(n, _) => n.start().await,
        }
    }

    pub async fn stop(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            Engine::Postgres(mut p, _) => p.stop().await,
            Engine::MongoDB(mut m, _) => m.stop().await,
            Engine::Neo4j(mut n, _) => n.stop().await,
        }
    }

    pub async fn monitor(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let engine = self.clone();

        spawn(async move {
            loop {
                match &engine {
                    Engine::Postgres(p, _) => p.monitor().await.unwrap(),
                    Engine::MongoDB(m, _) => m.monitor().await.unwrap(),
                    Engine::Neo4j(n, _) => n.monitor().await.unwrap(),
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
            Engine::Postgres(_, _) => Model::Relational,
            Engine::MongoDB(_, _) => Model::Document,
            Engine::Neo4j(_, _) => Model::Graph,
        }
    }

    fn current_load(&self) -> f64 {
        match self {
            Engine::Postgres(p, _) => p.current_load(),
            Engine::MongoDB(m, _) => m.current_load(),
            Engine::Neo4j(n, _) => n.current_load(),
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
