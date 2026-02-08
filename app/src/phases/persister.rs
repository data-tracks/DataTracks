use crate::management::Runtimes;
use crate::management::catalog::Catalog;
use crate::phases::{timer, wal};
use engine::engine::Engine;
use flume::{Receiver, unbounded};
use std::collections::HashMap;
use std::error::Error;
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep};
use tracing::{debug, error};
use util::definition::{Definition, Stage};
use util::{DefinitionId, Event, InitialMeta, PlainRecord, TargetedMeta, TimedMeta};
use value::Value;

pub struct Persister {
    catalog: Catalog,
}

const BATCH_SIZE: i32 = 100_000;

impl Persister {
    pub fn new(catalog: Catalog) -> anyhow::Result<Self> {
        Ok(Persister { catalog })
    }

    pub async fn move_to_engines(
        record: (Value, TimedMeta),
        engine: &mut [Engine],
        definitions: &mut [Definition],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (engine, value, meta) = Self::select_engines(record, engine, definitions).await?;

        debug!("store {} - {:?}", engine, value);

        engine.tx.send((value, meta))?;

        Ok(())
    }

    pub fn start(
        self,
        incoming: Receiver<(Value, InitialMeta)>,
        rt: Runtimes,
        control_rx: Receiver<u64>,
    ) {
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

    async fn select_engines<'a>(
        record: (Value, TimedMeta),
        engines: &'a mut [Engine],
        definitions: &mut [Definition],
    ) -> Result<(&'a Engine, Value, TargetedMeta), Box<dyn Error + Send + Sync>> {
        let definition = definitions
            .iter_mut()
            .find(|d| d.matches(&record.0, &record.1))
            .unwrap();

        let costs: Vec<_> = engines
            .iter()
            .map(|e| (e.cost(&record.0, definition), e))
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
                    if first_ts.elapsed().as_millis() > 200 || count > BATCH_SIZE {
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
}
