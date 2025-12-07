use crate::connection::PostgresConnection;
use bollard::models::{ContainerCreateBody, HostConfig, PortBinding};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, ListContainersOptionsBuilder,
    RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
};
use futures_util::stream::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::{ClientOptions, ServerApi, ServerApiVersion};
use mongodb::Database;
use neo4rs::{Config, Graph};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::format;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};
use tokio_postgres::Client;
use tracing::info;
use util::container::Manager;

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

struct MongoDB {
    client: Option<mongodb::Client>,
}

impl MongoDB {
    async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        start_container("engine-mongodb", "mongo:latest", 27017, 27017, None).await?;

        let uri = format!("mongodb://localhost:{}", 27017);
        let mut client_options = ClientOptions::parse(uri).await?;
        // Set the server_api field of the client_options object to Stable API version 1
        let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
        client_options.server_api = Some(server_api);
        // Create a new client and connect to the server
        let client = mongodb::Client::with_options(client_options)?;
        // Send a ping to confirm a successful connection
        timeout(
            Duration::from_secs(5),
            client.database("admin").run_command(doc! { "ping": 1 }),
        )
        .await
        .map(|res| ())
        .map_err(|err| format!("timeout after {}", err))?;
        info!("Connected to mongoDB database");

        self.client = Some(client);
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        stop("engine-mongodb").await
    }
}

struct Postgres {
    pub(crate) connector: PostgresConnection,
    client: Option<Client>,
}

impl Postgres {
    async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        start_container(
            "engine-postgres",
            "postgres:latest",
            5432,
            5432,
            Some(vec![format!("POSTGRES_PASSWORD={}", "postgres")]),
        )
        .await?;

        let client = self.connector.connect().await?;
        info!("Connected to postgres database");
        timeout(Duration::from_secs(5), client.check_connection()).await??;
        self.client = Some(client);
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        stop("engine-postgres").await
    }
}

struct Neo4j {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    graph: Option<Graph>,
}

impl Neo4j {
    async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        start_container(
            "engine-neo4j",
            "neo4j:latest",
            7687,
            7687,
            Some(vec![format!("NEO4J_AUTH=neo4j/{}", "neoneoneo")]),
        )
        .await?;

        let uri = format!("{}:{}", self.host, self.port);

        let graph = Graph::new(&uri, self.user.clone(), self.password.clone())?;

        let start_time = Instant::now();

        loop {
            match graph.run("MATCH (n) RETURN n").await {
                Ok(_) => break,
                Err(e) => {
                    let time = Instant::now();
                    if time.duration_since(start_time).as_secs() > 60 {
                        return Err(Box::new(e));
                    }
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }

        info!("Connected to postgres neo4j");
        self.graph = Some(graph);
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        stop("engine-neo4j").await
    }
}

async fn start_container(
    name: &str,
    image: &str,
    host_port: u16,
    container_port: u16,
    env_vars: Option<Vec<String>>,
) -> Result<(), Box<dyn Error>> {
    let docker = Manager::connect()?;

    let option = ListContainersOptionsBuilder::new().all(true).build();

    let list = docker.list_containers(Some(option)).await?;

    if list.into_iter().any(|c| {
        c.names
            .unwrap()
            .first()
            .into_iter()
            .any(|n| n.to_lowercase() == name)
    }) {
        stop(name).await?;
    }

    let options = CreateImageOptionsBuilder::new().from_image(image).build();

    let mut status = docker.create_image(Some(options), None, None);

    while let Some(msg) = status.try_next().await? {
        if let Some(status) = msg.status {
            info!("Pull Status: {}", status);
        }
    }

    // 1. Define the specific Host Port and IP
    let binding = PortBinding {
        // Listen on all host interfaces
        host_ip: Some("127.0.0.1".to_string()),
        // Map to host port 5432
        host_port: Some(host_port.to_string()),
    };

    // 2. Create the HostConfig's PortBindings map
    let mut port_bindings = HashMap::new();
    port_bindings.insert(
        format!("{}/{}", host_port.to_string(), "tcp"),
        Some(vec![binding]),
    );

    // 3. Create the HostConfig
    let host_config = HostConfig {
        port_bindings: Some(port_bindings),
        ..Default::default()
    };

    // 4. Create the ExposedPorts map for the container config
    let mut exposed_ports = HashMap::new();
    exposed_ports.insert(
        format!("{}/{}", container_port.to_string(), "tcp"),
        HashMap::new(),
    );

    let options = CreateContainerOptionsBuilder::new().name(name).build();

    let config = ContainerCreateBody {
        image: Some(image.to_string()),
        exposed_ports: Some(exposed_ports),
        host_config: Some(host_config),
        env: env_vars,
        ..Default::default()
    };

    docker.create_container(Some(options), config).await?;

    docker
        .start_container(name, None::<StartContainerOptions>)
        .await?;
    Ok(())
}

async fn stop(name: &str) -> Result<(), Box<dyn Error>> {
    let docker = Manager::connect()?;

    docker
        .stop_container(name, None::<StopContainerOptions>)
        .await?;
    docker
        .remove_container(name, None::<RemoveContainerOptions>)
        .await?;
    Ok(())
}
