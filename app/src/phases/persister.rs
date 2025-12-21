use crate::management::catalog::Catalog;
use engine::engine::Engine;
use engine::EngineKind;
use flume::{unbounded, Receiver};
use futures::StreamExt;
use num_format::{CustomFormat, Grouping, ToFormattedString};
use speedy::{Readable, Writable};
use statistics::Event;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use chrono::Utc;
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
}

impl Persister {
    pub async fn new(catalog: Catalog) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Ok(Persister {
            engines: vec![],
            catalog,
            last_log: RwLock::new(Instant::now()),
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
                    "Engines {} too long: {}",
                    engine,
                    engine.tx.len().to_formatted_string(&format)
                );
                let mut log = self.last_log.write().await;
                *log = Instant::now();
            }
        }
        engine.tx.send((value, context))?;

        Ok(())
    }

    pub async fn start(
        mut self,
        joins: &mut JoinSet<()>,
        mut incoming: Receiver<(Value, RecordContext)>,
    ) {
        let (sender, receiver) = unbounded();
        // timer
        joins.spawn(async move {
            let mut count = 0;
            loop {
                if count % 1_000 == 0 && incoming.len() > 100_0000 {
                    error!("Timer is overwhelmed! {}", incoming.len());
                }
                match incoming.recv_async().await {
                    Err(_) => {}
                    Ok((value, context)) => {
                        let record = WritableRecord::from((context,value));
                        sender.send(record).unwrap();
                    }
                }
                count += 1;
            }
        });


        let (tx, rx) = unbounded();
        // wal logger
        for i in 0..5 {
            let rx = receiver.clone();
            let tx = tx.clone();
            joins.spawn(async move {
                let mut log = SegmentedLog::new(&format!("wals/wal_segments_{}", i), 10 * 2048 * 2048).await.unwrap();
                loop {
                    let mut count = 0;

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
                    Ok(record) => {
                        self.move_to_engines(record.value, record.context).await.unwrap()
                    }
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

        debug!("costs:{:?}", costs.iter().map(|(k, v)| k).collect::<Vec<_>>());

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
        for mut engine in self.catalog.engines().await {
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
                                            match res {
                                                Ok(_) => None,
                                                Err(err) => Some(err)
                                            }
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
}

#[derive(Debug, Clone, Writable, Readable)]
pub struct WritableRecord {
    pub value: Value,
    pub timestamp: i64,
    pub context: RecordContext,
}


impl From<(RecordContext, Value)> for WritableRecord {
    fn from(value: (RecordContext, Value)) -> Self {
        WritableRecord{
            value: value.1,
            timestamp: Utc::now().timestamp_millis(),
            context: value.0,
        }
    }
}
