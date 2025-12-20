use comfy_table::presets::UTF8_FULL;
use comfy_table::Table;
use flume::{unbounded, Sender};
use num_format::{CustomFormat, ToFormattedString};
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::select;
use tokio::task::JoinSet;
use tokio::time::interval;

pub struct Statistics {
    engines: HashMap<String, EngineStatistic>,
}

impl Statistics {
    pub fn new() -> Self {
        Self {
            engines: Default::default(),
        }
    }

    async fn handle_event(&mut self, event: Event) {
        match event {
            Event::Insert(entity, amount, engine) => {
                self.engines
                    .entry(engine)
                    .or_default()
                    .handle_insert(amount, entity);
            }
        }
    }
}

pub async fn start(joins: &mut JoinSet<()>) -> Sender<Event> {
    let (tx, rx) = unbounded::<Event>();
    joins.spawn(async move {
        let mut statistics = Statistics::new();

        let mut timer = interval(Duration::from_secs(20));

        let initial = Instant::now();

        loop {
            select! {
                maybe_event = rx.recv_async() => {
                    match maybe_event {
                        Ok(event) => {
                            statistics.handle_event(event).await;
                        }
                        Err(_) => {}
                    }
                }
                _ = timer.tick() => {
                    println!{"{}", statistics.data(initial).unwrap()}
                }
            }
        }
    });
    tx
}

impl Statistics {
    fn data(&self, initial: Instant) -> Result<String, Box<dyn Error + Send + Sync>> {
        let mut table = Table::new();

        table.load_preset(UTF8_FULL);

        table.set_header(vec!["Entity", "Events"]);

        let format = CustomFormat::builder().separator("'").build()?;

        for (name, stats) in &self.engines {
            let mut info = "[".to_string();
            for (entity, amount) in &stats.handled_entities {
                info += &format!(
                    "{}:{} ",
                    entity,
                    amount.load(Ordering::Relaxed).to_formatted_string(&format)
                );
            }
            info += "]";
            table.add_row(vec![name.clone(), info.to_string()]);
        }

        let mut table_engines = Table::new();

        table_engines.load_preset(UTF8_FULL);

        table_engines.set_header(vec!["Engine", "Throughput/s"]);

        let epoch_delta = initial.elapsed().as_secs();

        for (name, stats) in &self.engines {
            let mut total = 0;
            for (_, amount) in &stats.handled_entities {
                total += amount.load(Ordering::Relaxed);
            }
            table_engines.add_row(vec![name.clone(), (total.div_ceil(epoch_delta as usize)).to_formatted_string(&format)]);
        }

        Ok(format!("{}\n{}", table, table_engines))
    }
}

#[derive(Default)]
pub struct EngineStatistic {
    handled_entities: HashMap<String, AtomicUsize>,
}

impl EngineStatistic {
    pub(crate) fn handle_insert(&mut self, amount: usize, entity: String) {
        self.handled_entities
            .entry(entity)
            .or_default()
            .fetch_add(amount, Ordering::Relaxed);
    }
}

pub enum Event {
    Insert(String, usize, String),
}
