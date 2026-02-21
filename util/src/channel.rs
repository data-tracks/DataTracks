use crate::event::{Event, QueueEvent};
use flume::Sender;
use num_format::{CustomFormat, ToFormattedString};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::info;

const WARNING: usize = 10_000;

static EVENT_SENDER: OnceLock<Sender<Event>> = OnceLock::new();

pub fn get_statistic_sender() -> Sender<Event> {
    EVENT_SENDER.get().unwrap().clone()
}

pub fn set_statistic_sender(sender: Sender<Event>) {
    EVENT_SENDER.set(sender).unwrap();
}

pub async fn log_channel<S: AsRef<str>, P: Send + 'static>(
    tx: Sender<P>,
    name: S,
    control_tx: Option<Sender<u64>>,
) {
    #[cfg(test)]
    {
        return;
    }

    let name = name.as_ref().to_string();
    let statistics = get_statistic_sender();

    tokio::spawn(async move {
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
                    tracing::error!(
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
                info!(
                    "Queue {} relaxed: {}",
                    name,
                    tx.len().to_formatted_string(&format)
                );
                overwhelmed.store(false, Ordering::Relaxed);
            }
        }
    });
}
