use crate::connection::PostgresConnection;
use crate::mongo::MongoDB;
use crate::neo::Neo4j;
use crate::postgres::Postgres;
use std::error::Error;
use std::fmt::{Display, Write};
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;
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

    pub async fn start_all() -> Result<Vec<Engine>, Box<dyn Error + Send + Sync>> {
        let mut engines: Vec<Engine> = vec![];

        let mut pg = Engine::postgres();
        pg.start().await?;
        pg.monitor().await?;
        pg.create_tables().await?;
        for _ in 0..1_000 {
            pg.insert_data().await?;
        }

        let mut mongodb = Engine::mongo_db();
        mongodb.start().await?;
        mongodb.monitor().await?;
        mongodb.create_collection().await?;
        for _ in 0..1_000 {
            mongodb.insert_data().await?;
        }

        let mut neo4j = Engine::neo4j();
        neo4j.start().await?;
        neo4j.monitor().await?;
        for _ in 0..1_000 {
            neo4j.insert_data().await?;
        }

        engines.push(pg.into());
        engines.push(mongodb.into());
        engines.push(neo4j.into());

        Ok(engines)
    }

    /// Mixture between current running tx, complexity of mapping (and user suggestion).
    pub fn cost(&self, value: &Value) -> f64 {
        match self {
            Engine::Postgres(p) => p.cost(value),
            Engine::MongoDB(m) => m.cost(value),
            Engine::Neo4j(n) => n.cost(value),
        }
    }

    /// Move pending operations on most efficient engine.
    pub fn reshaping(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        todo!()
    }

    /// Merge partitions or direct access to dynamically built view
    pub fn retrieve(
        &self,
        entity_name: String,
        query: String,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        todo!()
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            Engine::Postgres(p) => p.start().await,
            Engine::MongoDB(m) => m.start().await,
            Engine::Neo4j(n) => n.start().await,
        }
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            Engine::Postgres(p) => p.stop().await,
            Engine::MongoDB(m) => m.stop().await,
            Engine::Neo4j(n) => n.stop().await,
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

    pub fn postgres() -> Postgres {
        Postgres {
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
        MongoDB { client: None }
    }

    fn neo4j() -> Neo4j {
        Neo4j {
            host: "localhost".to_string(),
            port: 7687,
            user: "neo4j".to_string(),
            password: "neoneoneo".to_string(),
            database: "neo4j".to_string(),
            graph: None,
        }
    }
}
