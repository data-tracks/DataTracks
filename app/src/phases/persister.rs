use crate::management::catalog::Catalog;
use engine::engine::Engine;
use flume::{Receiver, unbounded};
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep};
use tracing::{debug, error};
use util::definition::{Definition, Stage};
use util::{DefinitionId, InitialMeta, PlainRecord, SegmentedLog, TargetedMeta, TimedMeta, log_channel, Event};
use value::Value;

pub struct Persister {
    engines: Vec<Engine>,
    catalog: Catalog,
}

impl Persister {
    pub async fn new(catalog: Catalog) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Ok(Persister {
            engines: vec![],
            catalog,
        })
    }

    pub async fn move_to_engines(
        &mut self,
        record: (Value, TimedMeta),
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (engine, value, meta) = self.select_engines(record).await?;

        debug!("store {} - {:?}", engine, value);

        engine.tx.send((value, meta))?;

        Ok(())
    }

    pub async fn start(
        mut self,
        joins: &mut JoinSet<()>,
        incoming: Receiver<(Value, InitialMeta)>,
    ) {
        let workers = 200;

        let dedicated_workers = 35;

        let id_queue = self.start_id_generator(joins, workers); // work stealing

        let (sender, receiver) = unbounded();
        log_channel(sender.clone(), "Timer");
        // timer
        for i in 0..workers {
            let incoming = incoming.clone();
            let sender = sender.clone();
            let id_queue = id_queue.clone();
            joins.spawn(async move {
                let mut available_ids = vec![];
                loop {
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
                        continue;
                    }

                    match incoming.recv_async().await {
                        Err(_) => {}
                        Ok((value, context)) => {
                            let id = available_ids.pop().unwrap(); // can unwrap, check above
                            let context = TimedMeta::new(id, context);
                            sender.send((value, context)).unwrap();
                        }
                    }
                }
            });
        }

        let (tx, rx) = unbounded();
        log_channel(tx.clone(), "WAL");

        // wal logger
        for i in 0..dedicated_workers {
            let rx = receiver.clone();
            let tx = tx.clone();

            // dedicated runtime in thread
            let wal_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            std::thread::spawn(move || {
                wal_runtime.block_on(async {
                    let mut log =
                        SegmentedLog::new(&format!("wals/wal_segments_{}", i), 200 * 2048 * 2048)
                            .await
                            .unwrap();

                    let mut batch = Vec::with_capacity(100_000);
                    loop {
                        match rx.recv() {
                            Err(_) => {}
                            Ok(record) => {
                                batch.push(record);
                                batch.extend(rx.try_iter().take(99_999));

                                log.log(&batch).await;

                                for record in batch.drain(..) {
                                    tx.send(record).unwrap();
                                }
                            }
                        }
                    }
                });
            });
        }

        // storer
        joins.spawn(async move {
            let mut engines = self.catalog.engines().await;

            self.engines.append(&mut engines);

            loop {
                match rx.recv_async().await {
                    Err(_) => {}
                    Ok(record) => self.move_to_engines(record).await.unwrap(),
                }
            }
        });
    }

    async fn select_engines(
        &self,
        record: (Value, TimedMeta),
    ) -> Result<(&Engine, Value, TargetedMeta), Box<dyn Error + Send + Sync>> {
        let definitions = self.catalog.definitions().await;

        let mut definition = Definition::empty();

        for mut d in definitions {
            if d.matches(&record.0, &record.1) {
                definition = d;
            }
        }

        let costs: Vec<_> = self
            .engines
            .iter()
            .map(|e| (e.cost(&record.0, &definition), e))
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

        Ok((cost.1, record.0, TargetedMeta::new(record.1, definition.id)))
    }

    pub async fn start_distributor(&mut self, joins: &mut JoinSet<()>) {
        for engine in self.catalog.engines().await {
            let mut clone = engine.clone();
            let definitions = self
                .catalog
                .definitions()
                .await
                .into_iter()
                .map(|d| (d.id, d))
                .collect::<HashMap<_, _>>();
            joins.spawn(async move {
                let mut error_count = 0;
                let mut count = 0;
                let mut first_ts = Instant::now();
                let mut buckets: HashMap<DefinitionId, Vec<(Value, TargetedMeta)>> = HashMap::new();

                let mut last_log = Instant::now();

                let engine_id = engine.id;

                loop {
                    if first_ts.elapsed().as_millis() > 200 || count > 1_000_000 {
                        // try to drain the "buffer"

                        for (id, records) in buckets.drain() {
                            let length = records.len();
                            let definition = definitions.get(&id).unwrap();
                            let name = definition.entity.plain.clone();
                            match clone.store(Stage::Plain, name, records.clone()).await {
                                Ok(_) => {
                                    engine.statistic_sender.send(Event::Insert(id, length, engine_id)).unwrap();
                                    definition.native.0.send_async(PlainRecord::new(records)).await.unwrap();
                                    error_count = 0;
                                    count = 0;
                                }
                                Err(err) => {
                                    error!("{:?}", err);
                                    error_count += 1;
                                    let errors: Vec<_> = records
                                        .into_iter()
                                        .filter_map(|v| {
                                            let res = engine
                                                .tx
                                                .send(
                                                    (v.0, v.1)
                                                )
                                                .map_err(|err| format!("{:?}", err));
                                            res.err()
                                        })
                                        .collect();

                                    if !errors.is_empty() {
                                        error!("{:?}", errors.first().unwrap())
                                    }

                                    if error_count > 1_000 && error_count < 10_000 && last_log.elapsed().as_secs() > 10 {
                                        error!("Error during distribution to engines over 1'000 tries, {} sleeping longer", err);
                                        last_log = Instant::now();
                                        sleep(Duration::from_millis(10)).await;
                                    } else if error_count > 10_000 {
                                        error!("Error during distribution to engines over 10'000 tries, {} shut down", err);
                                        panic!("Over 10'000 retries")
                                    }
                                }
                            }
                        }
                    }


                    match engine.rx.try_recv() {
                        Err(_) => sleep(Duration::from_millis(1)).await, // max shift after max timeout for sending finished chunk out
                        Ok(record) => {

                            if buckets.is_empty() {
                                first_ts = Instant::now();
                            }
                            buckets.entry(record.1.definition).or_default().push(record);
                            count += 1;
                        }
                    }
                }
            });
        }
    }

    fn start_id_generator(&self, join_set: &mut JoinSet<()>, workers: i32) -> Receiver<Vec<u64>> {
        let (tx, rx) = unbounded();
        log_channel(tx.clone(), "Id generator");

        let prepared_ids = (workers * 2) as usize;
        const ID_PACKETS_SIZE: u64 = 100_000;
        join_set.spawn(async move {
            // we prepare as much "id packets" as we have workers plus some more
            let mut count = 0u64;
            loop {
                if tx.len() > prepared_ids {
                    sleep(Duration::from_millis(50)).await;
                } else {
                    let mut ids: Vec<u64> = vec![ID_PACKETS_SIZE];
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
