use flume::Sender;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use num_format::{CustomFormat, ToFormattedString};
use tokio::spawn;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::info;

const WARNING: usize = 10_000;

pub fn log_channel<S: AsRef<str>, P: Send + 'static>(tx: Sender<P>, name: S) {
    let name = name.as_ref().to_string();
    spawn(async move {
        let last_log = RwLock::new(Instant::now());
        let overwhelmed = AtomicBool::new(false);

        let format = CustomFormat::builder().separator("'").build().unwrap();

        let mut interval = tokio::time::interval(Duration::from_secs(3));


        loop {
            interval.tick().await;
            if tx.len() > WARNING {
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