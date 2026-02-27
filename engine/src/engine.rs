use crate::connection::PostgresConnection;
use crate::mongo::MongoDB;
use crate::neo::Neo4j;
use crate::postgres::Postgres;
use derive_more::From;
use flume::{bounded, unbounded, Receiver, Sender};
use mongodb::bson::uuid;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Mul;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::sleep;
use util::definition::{Definition, Model, Stage};
use util::{
    log_channel, Batch, DefinitionId, EngineId, Event, PartitionId, SegmentedLogWriter,
    TargetedRecord,
};
use uuid::Uuid;
use value::Value;

static ID_BUILDER: AtomicU64 = AtomicU64::new(0);
const SEGMENT_SIZE: u64 = 10 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Engine {
    pub buffer_in: (Sender<TargetedRecord>, Receiver<TargetedRecord>),
    pub buffer_out: (Sender<TargetedRecord>, Receiver<TargetedRecord>),
    pub ids: Vec<u64>,
    pub statistic_sender: Sender<Event>,
    pub existing_partitions: Vec<(DefinitionId, PartitionId)>,
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
        let buffer_in = bounded(1_000_000);
        // we move blocking before the engine, away from the other engines
        let buffer_out = bounded(100_000);

        let id = EngineId(ID_BUILDER.fetch_add(1, Ordering::Relaxed));

        let name = format!("Engine-{}-{}", engine_kind, id.0);
        log_channel(buffer_out.0.clone(), name, None).await;

        let name = format!("Engine-{}-{}-buffer", engine_kind, id.0);
        log_channel(buffer_in.0.clone(), name, None).await;

        Engine {
            id,
            buffer_in,
            buffer_out,
            ids: vec![],
            statistic_sender: sender,
            existing_partitions: vec![],
            engine_kind,
            definitions: Default::default(),
        }
    }

    pub async fn start(&mut self, join: &mut JoinSet<()>, sender: Sender<Event>, is_new: bool) -> anyhow::Result<()> {
        let buffer_in_rx = self.buffer_in.1.clone();
        let buffer_out_tx = self.buffer_out.0.clone();

        let buffer_out_tx_skip = self.buffer_out.0.clone();

        let mut log = SegmentedLogWriter::new(
            format!("temp/engine/{}_{}.log", self.id.0, Uuid::new()).as_str(),
            SEGMENT_SIZE,
        )
        .await
        .unwrap();
        let reader = log.as_reader().await.unwrap();

        let buffer_size = Arc::new(AtomicU64::new(0));
        let buffer_size_recv = buffer_size.clone();

        let (index_tx, index_rx) = unbounded();
        // unlimited buffer
        thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                loop {
                    match buffer_in_rx.recv() {
                        Ok(record) => {
                            let mut values = vec![record];
                            values.extend(buffer_in_rx.try_iter().take(99_999));
                            if buffer_out_tx_skip.len() < 200_000
                                && index_tx.is_empty()
                                && buffer_size.load(Ordering::Relaxed) == 0
                            {
                                // we can send direct, nothing buffered, no buffer needed
                                for record in values {
                                    buffer_out_tx_skip.send(record).unwrap();
                                }
                            } else {
                                let record = log.log(&values).await;
                                let _ = index_tx.send(record.2);
                            }
                        }
                        Err(_) => break,
                    }
                }
            })
        });

        // holding feeder
        thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                loop {
                    match index_rx.recv() {
                        Ok(index) => {
                            // we have small window here where the no buffer approach could succeed, but at this point we do not care if elements are unordered
                            let mut indexes = vec![index];
                            buffer_size_recv.store(1, Ordering::Relaxed);
                            indexes.extend(index_rx.try_iter().take(99_999));

                            for index in indexes {
                                for record in reader.unlog(index).await {
                                    let _ = buffer_out_tx.send(record);
                                }
                            }

                            buffer_size_recv.store(0, Ordering::Relaxed);
                        }
                        Err(_) => break,
                    }
                }
            })
        });

        self.engine_kind.start(join, sender, is_new).await
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

        let pressure = self.buffer_out.1.len() + 1;

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
        partition_id: PartitionId,
        stage: Stage,
        definition_id: DefinitionId,
        values: &Batch<TargetedRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let ids = values
            .iter()
            .map(|TargetedRecord { value: _, meta }| meta.id)
            .collect::<Vec<_>>();
        self.ids.extend(ids.clone());

        let definition = self.definitions.get(&definition_id).unwrap();
        if !self
            .existing_partitions
            .contains(&(definition_id, partition_id))
        {
            self.engine_kind
                .init_entity(definition, partition_id)
                .await?;
            self.existing_partitions.push((definition_id, partition_id))
        }
        let entity_name = definition.entity_name(partition_id, &stage);

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
        partition_id: PartitionId,
    ) -> anyhow::Result<()> {
        match self {
            EngineKind::Postgres(p) => p.init_entity(definition, partition_id).await?,
            EngineKind::MongoDB(m) => m.init_entity(definition, partition_id).await?,
            EngineKind::Neo4j(n) => n.init_entity(definition, partition_id).await,
        };

        Ok(())
    }

    pub async fn start(
        &mut self,
        join: &mut JoinSet<()>,
        sender: Sender<Event>,
        is_new: bool,
    ) -> anyhow::Result<()> {
        match self {
            EngineKind::Postgres(p) => p.start(join, is_new).await?,
            EngineKind::MongoDB(m) => m.start(is_new).await?,
            EngineKind::Neo4j(n) => n.start(is_new).await?,
        }
        if is_new {
            self.monitor(join, sender.clone()).await?;
        }

        Ok(())
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
            engine.start(join, statistic_tx.clone(), true).await?;
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
