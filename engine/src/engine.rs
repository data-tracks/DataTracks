use crate::connection::PostgresConnection;
use crate::mongo::MongoDB;
use crate::neo::Neo4j;
use crate::postgres::Postgres;
use derive_more::From;
use flume::{Receiver, Sender, bounded, unbounded};
use futures_util::future::join_all;
use mongodb::bson::uuid;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::ops::Mul;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::{thread};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::warn;
use util::definition::{Definition, Model, Stage};
use util::{
    Batch, DefinitionId, EngineId, Event, PartitionId, QueueEvent, SegmentedLogWriter,
    TargetedRecord, log_channel,
};
use uuid::Uuid;
use value::Value;

static ID_BUILDER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct Engine {
    pub buffer_in: (Sender<TargetedRecord>, Receiver<TargetedRecord>),
    pub buffer_out: (Sender<Vec<TargetedRecord>>, Receiver<Vec<TargetedRecord>>),
    buffer_size: Arc<AtomicU64>,
    pub ids: Vec<u64>,
    pub statistic_sender: Sender<Event>,
    pub existing_partitions: Vec<(DefinitionId, PartitionId)>,
    pub engine_kind: EngineKind,
    pub id: EngineId,
    pub definitions: HashMap<DefinitionId, Definition>,
    pub handles: Vec<JoinHandle<()>>,
    pub logged: Arc<AtomicBool>,
}

impl Clone for Engine {
    fn clone(&self) -> Self {
        //let id = EngineId(ID_BUILDER.fetch_add(1, Ordering::Relaxed));
        Self {
            buffer_in: self.buffer_in.clone(),
            buffer_out: self.buffer_out.clone(),
            buffer_size: Arc::new(Default::default()),
            ids: vec![],
            statistic_sender: self.statistic_sender.clone(),
            existing_partitions: vec![],
            engine_kind: self.engine_kind.clone(),
            id: self.id.clone(),
            definitions: self.definitions.clone(),
            handles: vec![],
            logged: self.logged.clone(),
        }
    }
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
        let buffer_out = bounded(200_000);

        let id = EngineId(ID_BUILDER.fetch_add(1, Ordering::Relaxed));

        Engine {
            id,
            buffer_in,
            buffer_out,
            buffer_size: Arc::new(Default::default()),
            ids: vec![],
            statistic_sender: sender,
            existing_partitions: vec![],
            engine_kind,
            definitions: Default::default(),
            handles: vec![],
            logged: Arc::new(Default::default()),
        }
    }

    pub async fn start_container(&self) -> anyhow::Result<()> {
        match &self.engine_kind {
            EngineKind::Postgres(p) => p.start_container().await,
            EngineKind::MongoDB(m) => m.start_container().await,
            EngineKind::Neo4j(n) => n.start_container().await,
        }
    }

    pub async fn start(&mut self, join_set: &mut JoinSet<()>) -> anyhow::Result<()> {
        let buffer_in_rx = self.buffer_in.1.clone();

        let buffer_out_tx_skip = self.buffer_out.0.clone();

        let mut log = SegmentedLogWriter::new(
            format!("temp/engine/{}_{}", self.id.0, Uuid::new()).as_str(),
        )
        .await?;

        if self.logged.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok() {
            let name = format!("Engine-{}-{}", self.engine_kind, self.id);
            log_channel(self.buffer_out.0.clone(), name, None).await;

            let name = format!("Engine-{}-{}-buffer", self.engine_kind, self.id);
            log_channel(self.buffer_in.0.clone(), name, None).await;
        }

        let reader = Arc::new(log.build_reader().await?);
        let cleaner = log.build_cleaner();

        let (index_tx, index_rx) = unbounded();

        let buffer_size = self.buffer_size.clone();

        // unlimited buffer
        let handle = thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                loop {
                    if let Ok(record) = buffer_in_rx.recv() {
                        let mut values = vec![record];
                        values.extend(buffer_in_rx.try_iter().take(99_999));
                        if buffer_out_tx_skip.len() < 200_000
                            && index_tx.is_empty()
                            && buffer_size.load(Ordering::Relaxed) == 0
                        {
                            // we can send direct, nothing buffered, no buffer needed
                            buffer_out_tx_skip.send(values).unwrap();
                        } else {
                            let record = log.log(&values).await;
                            let _ = index_tx.send(record.2);
                            // warn!("direct insert {}", name_clone);
                        }
                    }
                }
            })
        });
        self.handles.push(handle);

        let statistic_sender = self.statistic_sender.clone();
        let name = format!("persister-file-{}", self.engine_kind);
        let buffer_out_tx = self.buffer_out.0.clone();

        // holding feeder
        let handle = thread::spawn(move || {
            let rt = Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                // Use a Semaphore to limit total concurrent disk reads
                // instead of a massive 200,000 buffer.
                let disk_semaphore = Arc::new(tokio::sync::Semaphore::new(100));
                let mut report_interval = tokio::time::interval(Duration::from_secs(3));
                report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);


                loop {
                    tokio::select! {
                        _ = report_interval.tick() => {
                            let _ = statistic_sender.send_async(Event::Queue(QueueEvent {
                                name: name.to_string(),
                                size: index_rx.len()
                            })).await;
                        }

                        // Directly pull from Flume
                        Ok(index) = index_rx.recv_async() => {
                            // 1. Quick Batching
                            let mut indexes = vec![index];
                            for _ in 0..999 {
                                if let Ok(idx) = index_rx.try_recv() {
                                    indexes.push(idx);
                                } else { break; }
                            }

                            // 2. Clone what we need for the background task
                            let reader = reader.clone();
                            let buffer_out_tx = buffer_out_tx.clone();
                            let sem = disk_semaphore.clone();

                            let cleaner = cleaner.cleaner_tx.clone();

                            // 3. Spawn the heavy lifting
                            tokio::spawn(async move {
                                let _permit = sem.acquire().await; // Throttle disk access

                                // Concurrent reads within the batch
                                let mut read_tasks = Vec::with_capacity(indexes.len());
                                for idx in indexes.iter() {
                                    read_tasks.push(reader.unlog(idx));
                                }

                                let results = join_all(read_tasks).await;

                                // Send results downstream
                                for data in results {
                                    if let Err(err) = buffer_out_tx.send(data) {
                                        warn!("Error sending data to buffer out channel: {}", err);
                                    }
                                }

                                // 4. Clean up without blocking the next read
                                for index in indexes {
                                    cleaner.send_async(index.segment_id).await.unwrap();
                                }
                            });
                        }
                    }
                }
            });
        });
        self.handles.push(handle);

        self.engine_kind.start(join_set, self.id).await
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

        let pressure = self.buffer_size.load(Ordering::Relaxed) + 1;

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
        values: Batch<TargetedRecord>,
    ) -> anyhow::Result<()> {
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
                .init_entity(definition, partition_id, &stage)
                .await?;

            self.existing_partitions.push((definition_id, partition_id))
        }
        let entity_name = definition.entity_name(partition_id, &stage);

        match &self.engine_kind {
            EngineKind::Postgres(p) => p.store(&stage, entity_name, &values).await,
            EngineKind::MongoDB(m) => m.store(&stage, entity_name, &values).await,
            EngineKind::Neo4j(n) => n.store(&stage, entity_name, &values).await,
        }
    }

    pub async fn read(&mut self, entity: String, ids: Vec<u64>) -> anyhow::Result<Vec<Value>> {
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
        stage: &Stage,
    ) -> anyhow::Result<()> {
        match self {
            EngineKind::Postgres(p) => p.init_entity(definition, partition_id, stage).await?,
            EngineKind::MongoDB(m) => m.init_entity(definition, partition_id, stage).await?,
            EngineKind::Neo4j(n) => n.init_entity(definition, partition_id).await,
        };

        Ok(())
    }

    pub async fn start(&mut self, join_set: &mut JoinSet<()>, id: EngineId) -> anyhow::Result<()> {
        match self {
            EngineKind::Postgres(p) => p.start(join_set, id).await?,
            EngineKind::MongoDB(m) => m.start(id).await?,
            EngineKind::Neo4j(n) => n.start(id).await?,
        }

        Ok(())
    }

    pub async fn get_all(statistic_tx: Sender<Event>) -> anyhow::Result<Vec<Engine>> {
        let engine_kinds: Vec<EngineKind> = vec![
            EngineKind::postgres().into(),
            EngineKind::mongo_db().into(),
            EngineKind::neo4j().into(),
        ];

        let init_futures = engine_kinds
            .into_iter()
            .map(|kind| Engine::new(kind, statistic_tx.clone()));
        Ok(join_all(init_futures).await)
    }

    pub async fn stop(self) -> anyhow::Result<()> {
        match self {
            EngineKind::Postgres(ref p) => p.stop().await,
            EngineKind::MongoDB(ref m) => m.stop().await,
            EngineKind::Neo4j(ref n) => n.stop().await,
        }
    }

    pub async fn monitor(
        &mut self,
        join_set: &mut JoinSet<()>,
        statistic_tx: Sender<Event>,
    ) -> anyhow::Result<()> {
        let mut engine = self.clone();
        engine.start(join_set, EngineId(0)).await?;

        join_set.spawn(async move {
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
            id: None,
            pg_id: crate::postgres::ID_BUILDER.fetch_add(1, Ordering::Relaxed),
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
            join: None,
        }
    }

    fn mongo_db() -> MongoDB {
        MongoDB {
            id: None,
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
            id: None,
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
