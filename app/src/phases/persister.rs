use crate::management::Runtimes;
use crate::management::catalog::Catalog;
use crate::phases::{timer, wal};
use anyhow::anyhow;
use engine::engine::Engine;
use flume::{Receiver, Sender, unbounded, bounded};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs, thread};
use std::path::PathBuf;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::select;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};
use util::definition::{Definition, Stage};
use util::{
    Batch, DefinitionId, Event, InitialRecord, PartitionId, TargetedMeta, TargetedRecord,
    TimedRecord, WorkerId,
};

pub struct Persister {
    catalog: Catalog,
    pub statistics_tx: Sender<Event>,
}

const BATCH_SIZE: i32 = 100_000; // between 50_000 and 100_000

const ENGINE_THREADS: i32 = 5;

impl Persister {
    pub fn new(catalog: Catalog, statistics_tx: Sender<Event>) -> anyhow::Result<Self> {
        Ok(Persister {
            catalog,
            statistics_tx,
        })
    }

    pub async fn send_to_engines(
        records: Vec<TimedRecord>,
        engine: &mut [Engine],
        definitions: &mut [Definition],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for record in records {
            let (engine, record) = Self::select_engine(record, engine, definitions).await?;

            debug!("store {} - {:?}", engine, record.value);

            engine.buffer_in.0.send_async(record).await?;
        }

        Ok(())
    }

    pub fn start(self, incoming: Receiver<InitialRecord>, rt: Runtimes, control_rx: Receiver<u64>) -> anyhow::Result<()> {
        let storer_workers = 8; // todo make dynamic

        let (sender, receiver) = unbounded();

        let control_rx = timer::handle_initial_time_annotation(incoming, &rt, sender, control_rx);

        let (wal_rx, _) =
            wal::handle_wal_to_engines(&rt, receiver, control_rx, self.statistics_tx.clone());

        let storer = thread::spawn(move || {
            let rt_storer = Builder::new_multi_thread()
                .worker_threads(storer_workers)
                .thread_name("storer")
                .enable_all()
                .build()
                .unwrap();

            rt_storer.block_on(async {
                let mut joins = JoinSet::new();

                for _ in 0..storer_workers {
                    let mut engines = self.catalog.engines().await;
                    let mut definitions = self.catalog.definitions().await;
                    let wal_rx_clone = wal_rx.clone();
                    joins.spawn(async move {
                        loop {
                            match wal_rx_clone.recv_async().await {
                                Err(_) => {}
                                Ok(record) => {
                                    let mut records = vec![];
                                    records.extend(record);
                                    records.extend(wal_rx_clone.try_iter().take(100).flatten());

                                    Self::send_to_engines(records, &mut engines, &mut definitions)
                                        .await
                                        .unwrap()
                                }
                            }
                        }
                    });
                }

                joins.join_all().await;
            });
        });
        rt.add_handle(storer);
        Ok(())
    }

    async fn select_engine<'a>(
        record: TimedRecord,
        engines: &'a mut [Engine],
        definitions: &mut [Definition],
    ) -> Result<(&'a Engine, TargetedRecord), Box<dyn Error + Send + Sync>> {
        let definition = definitions
            .iter_mut()
            .find(|d| d.matches(&record.value, &record.meta))
            .unwrap();

        let costs: Vec<_> = engines
            .iter()
            .map(|e| (e.cost(&record.value, definition), e))
            .collect();

        debug!(
            "costs:{:?}",
            costs.iter().map(|(k, _)| k).collect::<Vec<_>>()
        );

        let cost = costs
            .into_iter()
            .min_by(|a, b| a.0.total_cmp(&b.0))
            .unwrap();

        debug!("cost:{:?}", cost.1.engine_kind.to_string());

        Ok((
            cost.1,
            (record.value, TargetedMeta::new(record.meta, definition.id)).into(),
        ))
    }

    pub async fn start_distributor(&mut self, _join_set: &mut JoinSet<()>, _rt: Runtimes) -> anyhow::Result<()> {
        let engines = self.catalog.engines().await;

        let builder_id = Arc::new(AtomicU64::new(0));

        let total_workers = engines.len() * ENGINE_THREADS as usize;

        let (startup_tx, startup_rx) = bounded(total_workers);

        let path = PathBuf::from("temp/engine");

        if path.exists() {
            fs::remove_dir_all(path)?;
            warn!("Removed engines folders...")
        }

        for engine in engines {
            let engine_inner = engine.clone();
            let definitions = self.catalog.definitions().await;
            let builder_id = builder_id.clone();
            let startup_tx = startup_tx.clone();

            // Spawn a dedicated OS thread for this specific engine
            thread::spawn(move || {
                let rt = Builder::new_multi_thread()
                    .thread_name("persister")
                    .worker_threads(5) // No work-stealing possible
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let mut join_set = JoinSet::new();
                    for i in 0..ENGINE_THREADS {
                        let worker_id = builder_id.fetch_add(1, Ordering::Relaxed).into();
                        let mut engine = engine_inner.clone();
                        // actually make a new connection
                        engine.start(&mut join_set).await.unwrap();
                        startup_tx.send(()).unwrap();

                        let definitions = definitions
                            .clone()
                            .into_iter()
                            .map(|d| (d.id, d))
                            .collect::<HashMap<_, _>>();

                        tokio::spawn(async move {
                            let mut buckets: HashMap<DefinitionId, Batch<TargetedRecord>> = HashMap::new();
                            let mut count = 0;

                            let name = format!("Persister {} {}", engine, i);

                            let mut flush_interval = tokio::time::interval(Duration::from_millis(500));
                            // don't let ticks pile up if processing is slow
                            flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                            let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(ENGINE_THREADS as u64));
                            heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                            loop {
                                select! {
                                    biased;
                                     // Case C: Send Heartbeat
                                    _ = heartbeat_interval.tick() => {
                                         let _ = engine.statistic_sender.send(Event::Heartbeat(name.clone()));
                                    }
                                    // Case A: The timer hit 200ms
                                    _ = flush_interval.tick() => {
                                        if !buckets.is_empty() {
                                            flush_buckets(&worker_id, &mut buckets, &mut engine, &definitions).await;
                                            count = 0;
                                        }
                                    }

                                    // Case B: A record arrived
                                    Ok(records) = engine.buffer_out.1.recv_async() => {
                                        // Process the initial burst
                                        for record in records {
                                            buckets.entry(record.meta.definition).or_default().push(record);
                                            count += 1;
                                        }

                                        let mut extra_recvs = 0;
                                        while extra_recvs < 100 && count < BATCH_SIZE {
                                            if let Ok(extra_records) = engine.buffer_out.1.try_recv() {
                                                for record in extra_records {
                                                    buckets.entry(record.meta.definition).or_default().push(record);
                                                    count += 1;
                                                }
                                                extra_recvs += 1;
                                            } else {
                                                break;
                                            }
                                        }

                                        if count >= BATCH_SIZE {
                                            flush_buckets(&worker_id, &mut buckets, &mut engine, &definitions).await;
                                            count = 0;
                                            flush_interval.reset();
                                        }
                                    }
                                }
                            }
                        });
                    }
                    std::future::pending::<()>().await;
                });
            });
        }
        for _ in 0..total_workers {
            startup_rx.recv()?;
        }
        info!("All persisters started");
        Ok(())
    }
}

async fn flush_buckets(
    worker_id: &WorkerId,
    buckets: &mut HashMap<DefinitionId, Batch<TargetedRecord>>,
    engine: &mut Engine,
    definitions: &HashMap<DefinitionId, Definition>,
) {
    let mut error_count = 0u64;
    // We use drain() to take ownership of the Vecs without reallocating the HashMap memory
    for (id, records) in buckets.drain() {
        let definition = match definitions.get(&id) {
            Some(d) => d,
            None => {
                error!("DefinitionId {:?} not found in catalog", id);
                continue;
            }
        };

        let size = records.len() as u64;
        let source = engine.id;
        let partition_id = PartitionId(definition.partition_info.next(worker_id, &size));

        let mut last_log = Instant::now();

        let ids: Vec<u64> = records.iter().map(|r| r.meta.id).collect();

        match engine
            .store(partition_id, Stage::Plain, definition.id, &records)
            .await
        {
            Ok(_) => {
                let _ = engine
                    .statistic_sender
                    .send_async(Event::Insert {
                        id,
                        source,
                        ids,
                        stage: Stage::Plain,
                        first: Instant::now(),
                    })
                    .await;
                definition.native.0.send_async(records).await.unwrap();
                error_count = 0; // Reset errors on success
            }
            Err(err) => {
                handle_error(
                    anyhow!(err),
                    engine,
                    records,
                    &mut error_count,
                    &mut last_log,
                )
                .await;
            }
        }
    }
    // give it some breathing room
    tokio::task::yield_now().await;
}

async fn handle_error(
    err: anyhow::Error,
    engine: &Engine,
    records: Batch<TargetedRecord>,
    error_count: &mut u64,
    last_log: &mut Instant,
) {
    error!("Distribution error for engine {:?}: {:?}", engine.id, err);
    *error_count += 1;

    // 1. Backpressure/Sleep logic based on severity
    if *error_count > 1_000 && last_log.elapsed().as_secs() > 10 {
        warn!(
            "High error rate detected ({} tries). Throttling...",
            error_count
        );
        *last_log = Instant::now();
        // Use a small sleep to prevent "spinning" on a broken connection
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    if *error_count > 10_000 {
        // Critical failure: the engine is likely gone or the disk is full
        error!("Fatal: Over 10,000 retries. Shutting down worker.");
        panic!("Engine {:?} unrecoverable: {}", engine.id, err);
    }

    // 2. Data Recovery: Try to put records back into the engine's receiver
    // so they can be retried later.

    if let Err(send_err) = engine.buffer_out.0.send(records.records.to_vec()) {
        // If the internal channel is closed, we truly cannot save this data
        error!("Data loss: could not re-queue record: {:?}", send_err);
    }
}
