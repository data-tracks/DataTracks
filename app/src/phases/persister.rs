use crate::management::Runtimes;
use crate::management::catalog::Catalog;
use crate::phases::{timer, wal};
use anyhow::anyhow;
use engine::engine::Engine;
use flume::{Receiver, unbounded};
use std::collections::HashMap;
use std::error::Error;
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, error, warn};
use util::definition::{Definition, Stage};
use util::{Batch, DefinitionId, Event, InitialRecord, TargetedMeta, TargetedRecord, TimedRecord};

pub struct Persister {
    catalog: Catalog,
}

const BATCH_SIZE: i32 = 100_000;

impl Persister {
    pub fn new(catalog: Catalog) -> anyhow::Result<Self> {
        Ok(Persister { catalog })
    }

    pub async fn send_to_engines(
        record: TimedRecord,
        engine: &mut [Engine],
        definitions: &mut [Definition],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (engine, record) = Self::select_engines(record, engine, definitions).await?;

        debug!("store {} - {:?}", engine, record.value);

        engine.tx.send(record)?;

        Ok(())
    }

    pub fn start(self, incoming: Receiver<InitialRecord>, rt: Runtimes, control_rx: Receiver<u64>) {
        let storer_workers = 4;

        let (sender, receiver) = unbounded();

        let control_rx = timer::handle_initial_time_annotation(incoming, &rt, sender, control_rx);

        let (wal_rx, _) = wal::handle_wal_to_engines(&rt, receiver, control_rx);

        let storer = thread::spawn(move || {
            let rt_storer = Builder::new_current_thread()
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
                                    Self::send_to_engines(record, &mut engines, &mut definitions)
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
    }

    async fn select_engines<'a>(
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

    pub async fn start_distributor(&mut self, rt: Runtimes) {
        let engines = self.catalog.engines().await;
        let len = engines.len();

        let rt_persister = Builder::new_multi_thread()
            .worker_threads(len)
            .thread_name("persister")
            .enable_all()
            .build()
            .unwrap();

        for engine in engines {
            let mut clone = engine.clone();
            let definitions = self
                .catalog
                .definitions()
                .await
                .into_iter()
                .map(|d| (d.id, d))
                .collect::<HashMap<_, _>>();

            rt_persister.spawn(async move {
                let mut buckets: HashMap<DefinitionId, Batch<TargetedRecord>> = HashMap::new();
                let mut count = 0;

                let name = format!("Persister {}", engine);

                // Create a 200ms ticker for the flush interval
                let mut flush_interval = tokio::time::interval(Duration::from_millis(200));
                // don't let ticks pile up if processing is slow
                flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(500));
                heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        // Case A: The timer hit 200ms
                        _ = flush_interval.tick() => {
                            if !buckets.is_empty() {
                                flush_buckets(&mut buckets, &mut clone, &definitions).await;
                                count = 0;
                            }
                        }

                        // Case B: A record arrived
                        Ok(record) = engine.rx.recv_async() => {
                            buckets.entry(record.meta.definition).or_default().push(record);
                            count += 1;

                            // Immediate flush if we hit the batch size
                            if count >= BATCH_SIZE {
                                flush_buckets(&mut buckets, &mut clone, &definitions).await;
                                count = 0;
                                flush_interval.reset(); // Reset the timer since we just flushed
                            }
                        }

                        // Case C: Send Heartbeat (maybe on a different interval)
                        _ = heartbeat_interval.tick() => {
                             let _ = engine.statistic_sender.send(Event::Heartbeat(name.clone()));
                        }
                    }
                }
            });
        }

        rt.add_runtime(rt_persister);
    }
}

async fn flush_buckets(
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

        let size = records.len();
        let source = engine.id;
        let table_name = definition.entity.plain.clone();
        let partition_id = definition.partition_info.next(id, size);
        let mut last_log = Instant::now();

        let ids: Vec<u64> = records.iter().map(|r| r.meta.id).collect();

        match engine.store(Stage::Plain, table_name, &records).await {
            Ok(_) => {
                let _ = engine.statistic_sender.send(Event::Insert {
                    id,
                    size,
                    source,
                    ids,
                    stage: Stage::Plain,
                });
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
    for record in records {
        if let Err(send_err) = engine.tx.send(record) {
            // If the internal channel is closed, we truly cannot save this data
            error!("Data loss: could not re-queue record: {:?}", send_err);
            break;
        }
    }
}
