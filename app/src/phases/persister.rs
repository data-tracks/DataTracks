use crate::management::Runtimes;
use crate::management::catalog::Catalog;
use engine::engine::Engine;
use flume::{Receiver, Sender, unbounded};
use std::collections::HashMap;
use std::error::Error;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep};
use tracing::{debug, error};
use util::definition::{Definition, Stage};
use util::{
    DefinitionId, Event, InitialMeta, PlainRecord, SegmentedLog, TargetedMeta, TimedMeta,
    log_channel,
};
use value::Value;

pub struct Persister {
    catalog: Catalog,
}

impl Persister {
    pub fn new(catalog: Catalog) -> anyhow::Result<Self> {
        Ok(Persister { catalog })
    }

    pub async fn move_to_engines(
        record: (Value, TimedMeta),
        engine: &mut [Engine],
        definitions: &mut Vec<Definition>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (engine, value, meta) = Self::select_engines(record, engine, definitions).await?;

        debug!("store {} - {:?}", engine, value);

        engine.tx.send((value, meta))?;

        Ok(())
    }

    pub fn start(self, incoming: Receiver<(Value, InitialMeta)>, rt: Runtimes) {
        let timer_workers = 10;

        let wal_workers = 4;

        let storer_workers = 4;

        let (sender, receiver) = unbounded();
        let sender_clone = sender.clone();
        rt.attach_runtime(&0, async move {
            log_channel(sender_clone, "Timer -> WAL").await;
        });

        for _ in 0..1 {
            let timer = Self::start_timer(incoming.clone(), timer_workers, sender.clone());

            rt.add_handle(timer);
        }

        let (wal_tx, wal_rx) = unbounded();
        let wal_tx_clone = wal_tx.clone();
        rt.attach_runtime(&0, async move {
            log_channel(wal_tx_clone, "WAL -> Engines").await;
        });

        // wal logger
        for i in 0..wal_workers {
            let rx = receiver.clone();
            let tx = wal_tx.clone();

            let wal = thread::spawn(move || {
                // dedicated runtime in thread
                let wal_runtime = Builder::new_current_thread().enable_all().build().unwrap();

                wal_runtime.block_on(async {
                    let mut log =
                        SegmentedLog::new(&format!("wals/wal_segments_{}", i), 200 * 2048 * 2048)
                            .await
                            .unwrap();

                    let mut batch = Vec::with_capacity(100_000);
                    loop {
                        match rx.recv_async().await {
                            Err(err) => {
                                error!("Error in WAL: {}", err)
                            }
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
            rt.add_handle(wal);
        }

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
                                    Self::move_to_engines(record, &mut engines, &mut definitions)
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

    fn start_timer(
        incoming: Receiver<(Value, InitialMeta)>,
        timer_workers: usize,
        sender: Sender<(Value, TimedMeta)>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            // timer
            let rt_timer = Builder::new_multi_thread()
                .worker_threads(timer_workers)
                .thread_name("timer-processor")
                .enable_all()
                .build()
                .unwrap();

            rt_timer.block_on(async move {
                let mut joins: JoinSet<()> = JoinSet::new();
                let id_queue =
                    Self::start_id_generator(&mut joins, (timer_workers / 4) as i32).await; // work stealing

                for i in 0..timer_workers {
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
                                Err(_) => {
                                    error!("No incoming {}", i);
                                }
                                Ok((value, context)) => {
                                    let id = available_ids.pop().unwrap(); // can unwrap, check above
                                    let context = TimedMeta::new(id, context);
                                    sender.send((value, context)).unwrap();
                                }
                            }
                        }
                    });
                    // to distribute the "workers"
                    sleep(Duration::from_millis(50)).await;
                }
                joins.join_all().await;
            })
        })
    }

    async fn select_engines<'a>(
        record: (Value, TimedMeta),
        engines: &'a mut [Engine],
        definitions: &mut Vec<Definition>,
    ) -> Result<(&'a Engine, Value, TargetedMeta), Box<dyn Error + Send + Sync>> {
        let definition = definitions
            .into_iter()
            .find(|d| d.matches(&record.0, &record.1))
            .unwrap();

        let costs: Vec<_> = engines
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
                                    engine.statistic_sender.send(Event::Insert(id, length, engine_id, Stage::Plain)).unwrap();
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
                            debug!("current {}", engine.rx.len());

                            buckets.entry(record.1.definition).or_default().push(record);
                            count += 1;

                            if buckets.is_empty() {
                                first_ts = Instant::now();
                            }

                            let values = engine.rx.try_iter().take(999_999).collect::<Vec<_>>();

                            debug!("current after {}", engine.rx.len());

                            for record in values {
                                buckets.entry(record.1.definition).or_default().push(record);
                                count += 1;
                            }

                        }
                    }
                }
            });
        }
    }

    async fn start_id_generator(join_set: &mut JoinSet<()>, workers: i32) -> Receiver<Vec<u64>> {
        let (tx, rx) = unbounded();
        log_channel(tx.clone(), "Id generator").await;

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
