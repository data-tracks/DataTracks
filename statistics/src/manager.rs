use crate::web;
use flume::{Receiver, Sender, unbounded};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::spawn;
use std::time::Duration;
use tokio::runtime::{Builder, Handle};
use tokio::select;
use tokio::sync::broadcast;
use tokio::time::{interval, sleep};
use tracing::log::debug;
use util::Event::Runtime;
use util::definition::{Definition, Stage};
use util::{
    DefinitionId, EngineEvent, EngineId, Event, RuntimeEvent, Runtimes, StatisticEvent,
    log_channel, set_statistic_sender,
};

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
            Event::Insert(definition_id, amount, engine_id, stage) => {
                self.engines.entry(engine_id).or_default().handle_insert(
                    amount,
                    definition_id,
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

    pub(crate) fn get_summary(&self) -> Event {
        let names = &self.engine_names;
        let definition_names = self
            .definitions
            .iter()
            .map(|(id, d)| (id.0, d.name.clone()))
            .collect::<HashMap<_, _>>();
        Event::Statistics(StatisticEvent {
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
        })
    }
}

pub fn start(rt: Runtimes, tx: Sender<Event>, rx: Receiver<Event>) -> Sender<Event> {
    set_statistic_sender(tx.clone());

    let (status_tx, status_rx) = unbounded();

    let (bc_tx, _) = broadcast::channel(1_000_000);
    let clone_bc_tx = bc_tx.clone();

    let tx_clone = tx.clone();
    let statistic = spawn(move || {
        let tx = tx_clone.clone();
        let mut rt = Builder::new_current_thread()
            .thread_name("statistic-rt")
            .enable_all()
            .build()
            .unwrap();

        rt.spawn(async move {
            log_channel(tx_clone.clone(), "Events").await;

            let mut statistics = Statistics::new();

            let mut timer = interval(Duration::from_secs(20));

            loop {
                select! {
                    maybe_event = rx.recv_async() => {
                        if let Ok(event) = maybe_event {
                            statistics.handle_event(event.clone()).await;
                            let _res = clone_bc_tx.send(event);
                        }
                    },
                    _ = timer.tick() => {
                        if clone_bc_tx.send(statistics.get_summary()).is_err() {
                            debug!("Statistic ticks", )
                        }
                    }
                }
            }
        });
        web::start(&mut rt, bc_tx);

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
