use crate::{tpc, web};
use flume::{unbounded, Receiver, Sender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::{Duration};
use indexmap::IndexMap;
use tokio::runtime::{Builder, Handle};
use tokio::select;
use tokio::sync::broadcast;
use tokio::time::{interval, sleep, Instant, MissedTickBehavior};
use tracing::{error, warn};
use tracing::log::{debug};
use util::definition::{Definition, Stage};
use util::Event::Runtime;
use util::{log_channel, set_statistic_sender, Batch, DefinitionId, Delay, EngineEvent, EngineId, Event, RuntimeEvent, Runtimes, StatisticEvent, TargetedRecord, ThroughputEvent};

pub struct Statistics {
    engines: HashMap<EngineId, EngineStatistic>,
    engine_names: HashMap<EngineId, String>,
    definitions: HashMap<DefinitionId, Definition>,
    ids: IndexMap<u64, Instant>,
    delay: Delay,
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
            ids: Default::default(),
            delay: Default::default(),
        }
    }

    async fn handle_event(&mut self, event: Event) {
        match event {
            Event::Insert{id, first, source,ids, stage } => {
                self.engines.entry(source).or_default().handle_insert(
                    ids.len() as u64,
                    id,
                    stage.clone(),
                );

                match stage {
                    Stage::Timer => {
                        for id in ids {
                            self.ids.insert(id, first);
                        }
                    }
                    Stage::WAL => {}
                    Stage::Plain => {
                        self.delay.plain = ids.clone().into_iter().map(|id| {
                            if let Some(old) = self.ids.get_mut(&id) {
                                let duration = first.duration_since(old.clone());
                                duration
                            }else {
                                error!("mapped without plain");
                                Duration::from_secs(10000)
                            }
                        }).sum::<Duration>() / ids.len() as u32;
                    }
                    Stage::Mapped => {
                        self.delay.mapped = ids.clone().into_iter().map(|id| {
                            if let Some(old) = self.ids.get_mut(&id) {
                                let duration = first.duration_since(old.clone());
                                self.ids.shift_remove(&id);
                                duration
                            }else {
                                error!("mapped without plain");
                                Duration::from_secs(10000)
                            }
                        }).sum::<Duration>() / ids.len() as u32;
                    }
                }
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

    pub(crate) fn get_summary(&mut self) -> StatisticEvent {
        let names = &self.engine_names;
        let definition_names = self
            .definitions
            .iter()
            .map(|(id, d)| (id.0, d.topic.clone()))
            .collect::<HashMap<_, _>>();
        let event = StatisticEvent {
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
            delay: self.delay,
        };
        self.engines.clear();
        event
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
        let mut rt = Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("statistic-rt")
            .enable_all()
            .build()
            .unwrap();

        rt.spawn(async move {
            log_channel(tx_clone.clone(), "Events", None).await;

            let mut statistics = Statistics::new();

            let mut timer = interval(Duration::from_secs(20));
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let mut last_time = Instant::now();

            let mut last = statistics.get_summary();
            let mut current;

            loop {
                select! {
                    maybe_event = rx.recv_async() => {

                        if let Ok(event) = maybe_event {
                            let mut events = vec![event];

                            events.extend(rx.try_iter().take(99_999));

                            for event in events{
                                statistics.handle_event(event.clone()).await;
                                let _res = clone_bc_tx.send(event);
                            }
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
                        warn!("{:?}", statistics.delay);
                        warn!("open ids: {:?}", statistics.ids.len());
                        warn!("oldest: {:?}", statistics.ids.last().map(|id| id.1.clone()).unwrap_or(Instant::now()).elapsed());
                        last_time = Instant::now();
                        last = current.clone();
                        *last_shared_statistics_clone.lock().unwrap() = last.clone();
                        *last_shared_tp_clone.lock().unwrap() = ThroughputEvent{tps: throughput};
                    }
                }
            }
        });
        web::start(&mut rt, bc_tx.clone(), output, last_shared_statistic.clone(), last_shared_tp.clone());
        tpc::start(&mut rt, bc_tx, last_shared_statistic, last_shared_tp);

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
    handled_entities: HashMap<(DefinitionId, Stage), AtomicU64>,
}

impl EngineStatistic {
    pub(crate) fn to_stat(
        &self,
        definition_names: &HashMap<u64, String>,
    ) -> Vec<(DefinitionId, Stage, String, u64)> {
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

    pub(crate) fn handle_insert(&mut self, amount: u64, definition: DefinitionId, stage: Stage) {
        self.handled_entities
            .entry((definition, stage))
            .or_default()
            .fetch_add(amount, Ordering::Relaxed);
    }
}
