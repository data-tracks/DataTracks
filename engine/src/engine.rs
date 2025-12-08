use crate::connection::PostgresConnection;
use crate::mongo::MongoDB;
use crate::neo::Neo4j;
use crate::postgres::Postgres;
use std::error::Error;
use tokio::spawn;

pub enum Engine {
    Postgres(Postgres),
    MongoDB(MongoDB),
    Neo4j(Neo4j),
}

impl Engine {
    pub async fn start_all() -> Result<Vec<Engine>, Box<dyn Error>> {
        let mut engines = vec![];

        let mut pg = Engine::postgres();
        pg.start().await?;

        let mut mongodb = Engine::mongo_db();
        mongodb.start().await?;

        let mut neo4j = Engine::neo4j();
        neo4j.start().await?;

        engines.push(pg);
        engines.push(mongodb);
        engines.push(neo4j);


        Ok(engines)
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        match self {
            Engine::Postgres(p) => p.start().await,
            Engine::MongoDB(m) => m.start().await,
            Engine::Neo4j(n) => n.start().await,
        }
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        match self {
            Engine::Postgres(p) => p.stop().await,
            Engine::MongoDB(m) => m.stop().await,
            Engine::Neo4j(n) => n.stop().await,
        }
    }

    pub fn postgres() -> Self {
        Engine::Postgres(Postgres {
            connector: PostgresConnection {
                url: "localhost".to_string(),
                port: 5432,
                db: "postgres".to_string(),
                user: "postgres".to_string(),
                password: "postgres".to_string(),
            },
            client: None,
        })
    }

    fn mongo_db() -> Self {
        Engine::MongoDB(MongoDB { client: None })
    }

    fn neo4j() -> Self {
        Engine::Neo4j(Neo4j {
            host: "localhost".to_string(),
            port: 7687,
            user: "neo4j".to_string(),
            password: "neoneoneo".to_string(),
            database: "neo4j".to_string(),
            graph: None,
        })
    }
}
