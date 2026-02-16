use crate::web;
use flume::{unbounded, Receiver, Sender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::Duration;
use tokio::runtime::{Builder, Handle};
use tokio::select;
use tokio::sync::broadcast;
use tokio::time::{interval, sleep, Instant};
use tracing::log::debug;
use util::definition::{Definition, Stage};
use util::Event::Runtime;
use util::{log_channel, set_statistic_sender, Batch, DefinitionId, EngineEvent, EngineId, Event, RuntimeEvent, Runtimes, StatisticEvent, TargetedRecord, ThroughputEvent, ThroughputMeta};

pub struct Statistics {
    engines: HashMap<EngineId, EngineStatistic>,
    engine_names: HashMap<EngineId, String>,
    definitions: HashMap<DefinitionId, Definition>,
}

impl Default for Statistics {
    fn default() -> Self {
        Self::new()
    }
}

impl Statistics {
    pub fn new() -> Self {
        Self {
            engines: Default::default(),
            engine_names: Default::default(),
            definitions: Default::default(),
        }
    }

    async fn handle_event(&mut self, event: Event) {
        match event {
            Event::Insert{id, size, source, ids, stage} => {
                self.engines.entry(source).or_default().handle_insert(
                    size,
                    id,
                    stage,
                );
            }
            Event::Definition(definition_id, definition) => {
                self.definitions.insert(definition_id, *definition);
            }
            Event::Engine(engine_id, EngineEvent::Name(name)) => {
                self.engine_names.insert(engine_id, name);
                self.engines.insert(engine_id, EngineStatistic::default());
            }
            _ => {}
        }
    }

    pub(crate) fn get_summary(&self) -> StatisticEvent {
        let names = &self.engine_names;
        let definition_names = self
            .definitions
            .iter()
            .map(|(id, d)| (id.0, d.name.clone()))
            .collect::<HashMap<_, _>>();
        StatisticEvent {
            engines: self
                .engines
                .iter()
                .clone()
                .map(|(id, stat)| {
                    (
                        *id,
                        (stat.to_stat(&definition_names), names.get(id).cloned()),
                    )
                })
                .collect(),
        }
    }
}

pub fn start(rt: Runtimes, tx: Sender<Event>, rx: Receiver<Event>, output: broadcast::Sender<Batch<TargetedRecord>>) -> Sender<Event> {
    set_statistic_sender(tx.clone());

    let (status_tx, status_rx) = unbounded();

    let (bc_tx, _) = broadcast::channel(1_000_000);
    let clone_bc_tx = bc_tx.clone();

    let last_shared_statistic = Arc::new(Mutex::new(StatisticEvent::default()));
    let last_shared_tp = Arc::new(Mutex::new(ThroughputEvent::default()));

    let last_shared_statistics_clone = last_shared_statistic.clone();
    let last_shared_tp_clone = last_shared_tp.clone();

    let tx_clone = tx.clone();
    let statistic = spawn(move || {
        let tx = tx_clone.clone();
        let mut rt = Builder::new_current_thread()
            .thread_name("statistic-rt")
            .enable_all()
            .build()
            .unwrap();

        rt.spawn(async move {
            log_channel(tx_clone.clone(), "Events", None).await;

            let mut statistics = Statistics::new();

            let mut timer = interval(Duration::from_secs(20));

            let mut last_time = Instant::now();

            let mut last = statistics.get_summary();
            let mut current;

            loop {
                select! {
                    maybe_event = rx.recv_async() => {
                        if let Ok(event) = maybe_event {
                            statistics.handle_event(event.clone()).await;
                            let _res = clone_bc_tx.send(event);
                        }
                    },
                    _ = timer.tick() => {
                        let since = Instant::now().duration_since(last_time);
                        current = statistics.get_summary();
                        let throughput = current.calculate(last.clone(), since);

                        if clone_bc_tx.send(Event::Statistics(last)).is_err() {
                            debug!("Statistic ticks", )
                        }
                        
                        if clone_bc_tx.send(Event::Throughput(ThroughputEvent{tps: throughput.clone()})).is_err() {
                            debug!("Statistic throughput ticks", )
                        }

                        last_time = Instant::now();
                        last = current.clone();
                        *last_shared_statistics_clone.lock().unwrap() = last.clone();
                        *last_shared_tp_clone.lock().unwrap() = ThroughputEvent{tps: throughput};
                    }
                }
            }
        });
        web::start(&mut rt, bc_tx, output, last_shared_statistic, last_shared_tp);

        let statistic_tx = tx.clone();

        status_tx.send(true).unwrap();
        rt.block_on(async move {
            let metrics = Handle::current().metrics();

            loop {
                statistic_tx
                    .send_async(Runtime(RuntimeEvent {
                        active_tasks: metrics.num_alive_tasks(),
                        worker_threads: metrics.num_workers(),
                        blocking_threads: metrics.num_blocking_threads(),
                        budget_forces_yield: metrics.budget_forced_yield_count() as usize,
                    }))
                    .await
                    .unwrap();
                sleep(Duration::from_secs(5)).await;
            }
        });
    });
    status_rx.recv().unwrap();

    rt.add_handle(statistic);

    tx
}

#[derive(Default)]
pub struct EngineStatistic {
    handled_entities: HashMap<(DefinitionId, Stage), AtomicUsize>,
}

impl EngineStatistic {
    pub(crate) fn to_stat(
        &self,
        definition_names: &HashMap<u64, String>,
    ) -> Vec<(DefinitionId, Stage, String, usize)> {
        self.handled_entities
            .iter()
            .map(|((k, stage), v)| {
                (
                    *k,
                    stage.clone(),
                    definition_names.get(&k.0).cloned().unwrap(),
                    v.load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    pub(crate) fn handle_insert(&mut self, amount: usize, definition: DefinitionId, stage: Stage) {
        self.handled_entities
            .entry((definition, stage))
            .or_default()
            .fetch_add(amount, Ordering::Relaxed);
    }
}
