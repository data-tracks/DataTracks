use crate::{tpc, web};
use flume::{unbounded, Receiver, Sender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::{Duration};
use indexmap::IndexMap;
use num_format::{CustomFormat, ToFormattedString};
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
            Event::Insert { id, first, source, ids, stage } => {
                // 1. Handle engine stats
                self.engines.entry(source).or_default().handle_insert(
                    ids.len() as u64,
                    id,
                    stage.clone(),
                );

                // 2. Early exit if ids is empty to prevent division by zero
                if ids.is_empty() { return; }
                let count = ids.len() as u32;

                match stage {
                    Stage::Timer => {
                        for id in ids {
                            self.ids.insert(id, first);
                        }
                    }
                    Stage::Plain => {
                        let total_dur: Duration = ids.iter().map(|id| {
                            self.ids.get(id).map(|old| first.duration_since(*old))
                                .unwrap_or_else(|| {
                                    error!("Plain without Timer for ID: {}", id);
                                    Duration::from_secs(0)
                                })
                        }).sum();
                        self.delay.plain = total_dur / count;
                    }
                    Stage::Mapped => {
                        let (total_dur, max_dur) = ids.iter().fold(
                            (Duration::from_secs(0), Duration::from_secs(0)),
                            |(sum, max),id| {
                            // Use swap_remove for O(1) performance!
                            let dur = self.ids.swap_remove(id)
                                .map(|old| first.duration_since(old))
                                .unwrap_or_else(|| {
                                    error!("Mapped without Timer/Plain for ID: {}", id);
                                    Duration::from_secs(0)
                                });
                                (sum + dur, std::cmp::max(max, dur))
                        });
                        self.delay.mapped = total_dur / count;
                        self.delay.max = max_dur;
                    }
                    _ => {}
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
                .filter_map(|(id, stat)| {
                    if let Some(name) = names.get(id) {
                        Some((
                            *id,
                            (stat.to_stat(&definition_names), Some(name.to_string())),
                        ))
                    }else {
                        None
                    }
                })
                .collect(),
            delay: self.delay,
        };
        for value in self.engines.values_mut() {
            for count in value.handled_entities.values_mut() {
                count.store(0, Ordering::Relaxed);
            }
        }
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
        let rt = Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("statistic-rt")
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {

            let stats_handle = tokio::spawn(async move {
                log_channel(tx_clone.clone(), "Events", None).await;

                let mut statistics = Statistics::new();

                let mut timer = interval(Duration::from_secs(20));
                timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

                let mut last_time = Instant::now();

                let mut last = statistics.get_summary();
                let mut current;

                let format = CustomFormat::builder().separator("'").build().unwrap();

                loop {
                    select! {
                        _ = timer.tick() => {
                            let since = last_time.elapsed();
                            current = statistics.get_summary();
                            let throughput = current.calculate(since);

                            if let Err(_) = clone_bc_tx.send(Event::Statistics(last.clone())) {
                                debug!("Statistic broadcast lag or no subscribers");
                            }

                            if let Err(_) = clone_bc_tx.send(Event::Throughput(ThroughputEvent{tps: throughput.clone()})) {
                                debug!("Throughput broadcast lag");
                            }

                            warn!("Stats Update: {} open IDs | Oldest: {:?}",
                                statistics.ids.len().to_formatted_string(&format),
                                statistics.ids.last().map(|id| id.1.elapsed()).unwrap_or_default()
                            );

                            last_time = Instant::now();
                            last = current;

                            // Critical: Keep lock duration as short as possible
                            if let Ok(mut stats_lock) = last_shared_statistics_clone.try_lock() {
                                *stats_lock = last.clone();
                            }
                            if let Ok(mut tp_lock) = last_shared_tp_clone.try_lock() {
                                *tp_lock = ThroughputEvent{tps: throughput};
                            }
                        },

                        maybe_event = rx.recv_async() => {
                            match maybe_event {
                                Ok(event) => {
                                    // Process the first event
                                    statistics.handle_event(event.clone()).await;
                                    let _ = clone_bc_tx.send(event);

                                    let mut count = 0;
                                    while let Ok(next_event) = rx.try_recv() {
                                        statistics.handle_event(next_event.clone()).await;
                                        let _ = clone_bc_tx.send(next_event);

                                        count += 1;
                                        if count >= 5_000 {
                                            // Force a yield to allow the timer to trigger
                                            tokio::task::yield_now().await;
                                            break;
                                        }
                                    }
                                }
                                Err(_) => break, // Channel closed
                            }
                        }
                    }
                }
                error!("stopped here")
            });
            web::start(bc_tx.clone(), output, last_shared_statistic.clone(), last_shared_tp.clone());
            tpc::start(bc_tx, last_shared_statistic, last_shared_tp);

            let statistic_tx = tx.clone();

            status_tx.send(true).unwrap();
            let metrics_handle = tokio::spawn(async move {
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

            let _ = tokio::join!(stats_handle, metrics_handle);

        });
    });
    status_rx.recv().unwrap();

    rt.add_handle(statistic);

    tx
}

#[derive(Default)]
pub struct EngineStatistic {
    pub(crate) handled_entities: HashMap<(DefinitionId, Stage), AtomicU64>,
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
