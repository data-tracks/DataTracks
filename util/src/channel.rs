use crate::event::{Event, QueueEvent};
use flume::Sender;
use num_format::{CustomFormat, ToFormattedString};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use once_cell::sync::Lazy;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{debug, warn};

const WARNING: usize = 10_000;

static EVENT_SENDER: OnceLock<Sender<Event>> = OnceLock::new();

static MONITOR_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Builder::new_multi_thread()
        .worker_threads(2) // Keep it lean
        .thread_name("log-monitor-runtime")
        .enable_all()
        .build()
        .expect("Failed to create dedicated runtime")
});

pub fn get_statistic_sender() -> Option<Sender<Event>> {
    EVENT_SENDER.get().cloned()
}

pub fn set_statistic_sender(sender: Sender<Event>) {
    EVENT_SENDER.set(sender).unwrap();
}

pub async fn log_channel<S: AsRef<str>, P: Send + 'static>(
    tx: Sender<P>,
    name: S,
    control_tx: Option<Sender<u64>>,
) {


    let name = name.as_ref().to_string();
    let statistics = if let Some(statistics) = get_statistic_sender(){
        statistics
    }else {
        warn!("No sender for channel logging");
        return;
    };

    MONITOR_RUNTIME.spawn(async move {
        let last_log = RwLock::new(Instant::now());
        let overwhelmed = AtomicBool::new(false);

        let format = CustomFormat::builder().separator("'").build().unwrap();
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        let mut len;

        loop {
            interval.tick().await;
            len = tx.len();
            statistics
                .send(Event::Queue(QueueEvent {
                    name: name.clone(),
                    size: len,
                }))
                .unwrap();
            if len > WARNING {
                let do_log = last_log.read().await.elapsed() > Duration::from_secs(10);
                if do_log {
                    debug!(
                        "Queue {} too big: {}",
                        name,
                        tx.len().to_formatted_string(&format)
                    );
                    let mut log = last_log.write().await;
                    *log = Instant::now();
                    overwhelmed.store(true, Ordering::Relaxed);
                }
                if let Some(tx) = &control_tx {
                    tx.send(len as u64).unwrap();
                }
            } else if overwhelmed.load(Ordering::Relaxed) {
                debug!(
                    "Queue {} relaxed: {}",
                    name,
                    tx.len().to_formatted_string(&format)
                );
                overwhelmed.store(false, Ordering::Relaxed);
            }
        }
    });
}
