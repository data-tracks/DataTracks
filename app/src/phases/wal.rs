use flume::{Receiver, Sender};
use std::collections::HashMap;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;
use tracing::info;
use util::{SegmentedLog, TimedMeta};
use value::Value;

struct WalWorker {
    handle: std::thread::JoinHandle<()>,
    cancel_token: CancellationToken,
}

pub struct WalManager {
    workers: Vec<(u64, WalWorker)>,
    next_id: u64,
}

impl WalManager {
    pub(crate) fn new() -> Self {
        Self {
            workers: Default::default(),
            next_id: 0,
        }
    }

    pub fn add_worker(&mut self, rx: Receiver<(Value, TimedMeta)>, tx: Sender<(Value, TimedMeta)>) {
        let id = self.next_id;
        let token = CancellationToken::new();
        let worker_token = token.clone();

        let handle = std::thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let mut log = SegmentedLog::new(&format!("wals/wal_{}", id), 200 * 2048 * 2048)
                    .await
                    .unwrap();
                let mut batch = Vec::with_capacity(100_000);

                loop {
                    tokio::select! {
                        // EXIT SIGNAL: The manager called for a shrink
                        _ = worker_token.cancelled() => {
                            info!("WAL Worker {} shutting down gracefully", id);
                            return;
                        }

                        // WORK LOGIC
                        res = rx.recv_async() => {
                            match res {
                                Ok(record) => {
                                    batch.push(record);
                                    batch.extend(rx.try_iter().take(99_999));
                                    log.log(&batch).await;
                                    for r in batch.drain(..) { tx.send(r).unwrap(); }
                                }
                                Err(_) => return, // Channel closed
                            }
                        }
                    }
                }
            });
        });

        self.workers.push((
            id,
            WalWorker {
                handle,
                cancel_token: token,
            },
        ));
        self.next_id += 1;
    }

    pub fn remove_worker(&mut self) {
        if let Some((id, worker)) = self.workers.pop() {
            worker.cancel_token.cancel();
            let _ = worker.handle.join();
            info!("WAL Worker {} removed.", id);
        }
    }
}
