use crate::management::catalog::Catalog;
use chrono::Utc;
use engine::engine::Engine;
use engine::EngineKind;
use flume::{unbounded, Receiver, RecvError};
use futures::StreamExt;
use num_format::{CustomFormat, Grouping, ToFormattedString};
use speedy::{Readable, Writable};
use statistics::Event;
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use num_format::Locale::sl;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info};
use util::definition::Definition;
use util::queue::{Meta, RecordContext, RecordQueue};
use util::SegmentedLog;
use value::Value;

pub struct Persister {
    engines: Vec<Engine>,
    catalog: Catalog,
    last_log: RwLock<Instant>,
    overwhelmed: AtomicBool,
}

impl Persister {
    pub async fn new(catalog: Catalog) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Ok(Persister {
            engines: vec![],
            catalog,
            last_log: RwLock::new(Instant::now()),
            overwhelmed: AtomicBool::new(false),
        })
    }

    pub async fn move_to_engines(
        &mut self,
        value: Value,
        context: RecordContext,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (engine, context) = self.select_engines(&value, context).await?;

        debug!("store {} - {}", engine, value);
        if engine.tx.len() > 10_000 {
            let format = CustomFormat::builder().separator("'").build()?;

            let do_log = self.last_log.read().await.elapsed() > Duration::from_secs(10);
            if do_log {
                error!(
                    "Engine queue {} too big: {}",
                    engine,
                    engine.tx.len().to_formatted_string(&format)
                );
                let mut log = self.last_log.write().await;
                *log = Instant::now();
                self.overwhelmed.store(true, Ordering::Relaxed);
            }
        }else if self.overwhelmed.load(Ordering::Relaxed) {
            let format = CustomFormat::builder().separator("'").build()?;
                info!(
                    "Engine queue {} relaxed: {}",
                    engine,
                    engine.tx.len().to_formatted_string(&format)
                );
            self.overwhelmed.store(false, Ordering::Relaxed);
        }
        engine.tx.send((value, context))?;

        Ok(())
    }

    pub async fn start(
        mut self,
        joins: &mut JoinSet<()>,
        incoming: Receiver<(Value, RecordContext)>,
    ) {
        let workers = 10;

        let id_queue = self.start_id_generator(joins, workers); // work stealing

        let (sender, receiver) = unbounded();
        // timer
        for i in 0..workers {
            let incoming = incoming.clone();
            let sender = sender.clone();
            let id_queue = id_queue.clone();
            joins.spawn(async move {
                let mut count = 0;

                let mut available_ids= vec![];
                loop {
                    if count % 1_000 == 0 && incoming.len() > 100_0000 {
                        error!("Timer is overwhelmed! {}", incoming.len());
                    }
                    if available_ids.is_empty() {
                        match id_queue.recv_async().await {
                            Ok(ids) => available_ids.extend(ids),
                            Err(_) => {
                                error!("No available ids in worker {}", i);
                                sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        }
                    }
                    if available_ids.is_empty() {
                        error!("No available ids in worker {}", i);
                        sleep(Duration::from_millis(50)).await;
                        continue
                    }

                    match incoming.recv_async().await {
                        Err(_) => {}
                        Ok((value, context)) => {
                            let id = available_ids.pop().unwrap(); // can unwrap, check above
                            let record = WritableRecord::from((id, context, value));
                            sender.send(record).unwrap();
                        }
                    }
                    count += 1;
                }
            });
        }

        let (tx, rx) = unbounded();
        // wal logger
        for i in 0..workers {
            let rx = receiver.clone();
            let tx = tx.clone();
            joins.spawn(async move {
                let mut log =
                    SegmentedLog::new(&format!("wals/wal_segments_{}", i), 10 * 2048 * 2048)
                        .await
                        .unwrap();
                let mut count = 0;
                loop {
                    if count % 1_000 == 0 && rx.len() > 100_0000 {
                        error!("WAL is overwhelmed! {}", rx.len());
                    }
                    match rx.recv_async().await {
                        Err(_) => {}
                        Ok(record) => {
                            Self::log(&mut log, record.clone()).await;
                            tx.send(record).unwrap();
                        }
                    }
                    count += 1;
                }
            });
        }

        // storer
        joins.spawn(async move {
            let mut engines = self.catalog.engines().await;

            self.engines.append(&mut engines);

            loop {
                match rx.recv_async().await {
                    Err(_) => {}
                    Ok(record) => self
                        .move_to_engines(record.value, record.context)
                        .await
                        .unwrap(),
                }
            }
        });
    }

    async fn log(log: &mut SegmentedLog, record: WritableRecord) {
        let mut bytes = record.write_to_vec().unwrap();
        bytes.push(b'\n');
        log.write(bytes.as_slice()).await;
    }

    async fn select_engines(
        &self,
        value: &Value,
        context: RecordContext,
    ) -> Result<(&Engine, RecordContext), Box<dyn Error + Send + Sync>> {
        let definitions = self.catalog.definitions().await;

        let mut definition = Definition::empty();

        for mut d in definitions {
            if d.matches(value, &context.meta) {
                definition = d;
            }
        }

        let costs: Vec<_> = self
            .engines
            .iter()
            .map(|e| (e.cost(value, &definition), e))
            .collect();

        debug!(
            "costs:{:?}",
            costs.iter().map(|(k, v)| k).collect::<Vec<_>>()
        );

        let cost = costs
            .into_iter()
            .min_by(|a, b| a.0.total_cmp(&b.0))
            .unwrap();

        debug!("cost:{:?}", cost.1.engine_kind.to_string());

        Ok((
            cost.1,
            RecordContext {
                meta: context.meta,
                entity: Some(definition.entity),
            },
        ))
    }

    pub async fn start_distributor(&mut self, joins: &mut JoinSet<()>) {
        for engine in self.catalog.engines().await {
            let clone = engine.clone();

            joins.spawn(async move {
                let mut error_count = 0;
                let mut count = 0;
                let mut first_ts = Instant::now();
                let mut buckets: HashMap<String, Vec<Value>> = HashMap::new();

                let mut last_log = Instant::now();

                let name = engine.to_string();

                loop {
                    if first_ts.elapsed().as_millis() > 200 || count > 1_000_000 {
                        // try to drain the "buffer"

                        for (entity, values) in buckets.drain() {
                            let length = values.len();
                            match clone.store(entity.clone(), values.clone()).await {
                                Ok(_) => {
                                    engine.statistic_sender.send(Event::Insert(entity, length, name.to_string())).unwrap();
                                    error_count = 0;
                                    count = 0;
                                }
                                Err(err) => {
                                    error_count += 1;
                                    let errors: Vec<_> = values
                                        .into_iter()
                                        .filter_map(|v| {
                                            let res = engine
                                                .tx
                                                .send((
                                                    v,
                                                    RecordContext::new(
                                                        Meta::new(Some(entity.clone())),
                                                        entity.clone(),
                                                    ),
                                                ))
                                                .map_err(|err| format!("{:?}", err));
                                            res.err()
                                        })
                                        .collect();

                                    if !errors.is_empty() {
                                        error!("{}", errors.first().unwrap())
                                    }

                                    if error_count > 1_000 && error_count < 10_000 && last_log.elapsed().as_secs() > 10 {
                                        error!("Error during distribution to engines over 1'000 tries, {} sleeping longer", err);
                                        last_log = Instant::now();
                                        sleep(Duration::from_millis(10)).await;
                                    } else if error_count > 10_000 {
                                        error!("Error during distribution to engines over 10'000 tries, {} shut down", err);
                                        panic!("Over 1 Mio retries")
                                    }
                                }
                            }
                        }
                    }


                    match engine.rx.try_recv() {
                        Err(_) => sleep(Duration::from_millis(1)).await, // max shift after max timeout for sending finished chunk out
                        Ok((v, context)) => {
                            let entity = context.entity.unwrap_or("_stream".to_string());

                            if buckets.is_empty() {
                                first_ts = Instant::now();
                            }
                            buckets.entry(entity).or_default().push(v);
                            count += 1;
                        }
                    }
                }
            });
        }
    }

    fn start_id_generator(&self, join_set: &mut JoinSet<()>, workers: i32) -> Receiver<Vec<usize>> {
        let (tx, rx) = unbounded();

        let prepared_ids = (workers * 2) as usize;
        const ID_PACKETS_SIZE: usize = 100_000;
        join_set.spawn(async move {
            // we prepare as much "id packets" as we have workers plus some more
            let mut count = 0usize;
            loop {
                if tx.len() > prepared_ids {
                    sleep(Duration::from_millis(50)).await;
                } else {
                    let mut ids: Vec<usize> = vec![ID_PACKETS_SIZE];
                    for _ in 0..ID_PACKETS_SIZE {
                        ids.push(count);
                        count += 1;
                    }
                    tx.send(ids).unwrap();
                }
            }
        });
        rx
    }
}

#[derive(Debug, Clone, Writable, Readable)]
pub struct WritableRecord {
    pub id: usize,
    pub value: Value,
    pub timestamp: i64,
    pub context: RecordContext,
}

impl From<(usize, RecordContext, Value)> for WritableRecord {
    fn from(value: (usize, RecordContext, Value)) -> Self {
        WritableRecord {
            id: value.0,
            value: value.2,
            timestamp: Utc::now().timestamp_millis(),
            context: value.1,
        }
    }
}
