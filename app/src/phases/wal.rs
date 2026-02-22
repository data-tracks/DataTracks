use flume::{unbounded, Receiver, Sender};
use std::thread;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;
use tracing::info;
use util::{log_channel, Runtimes, SegmentedLog, TimedRecord};

struct WalWorker {
    handle: thread::JoinHandle<()>,
    cancel_token: CancellationToken,
}

pub struct WalManager {
    workers: Vec<(u64, WalWorker)>,
    next_id: u64,
}

impl WalManager {
    pub(crate) fn workers(&self) -> usize {
        self.workers.len()
    }

    pub(crate) fn new() -> Self {
        Self {
            workers: Default::default(),
            next_id: 0,
        }
    }

    pub fn add_worker(&mut self, rx: Receiver<TimedRecord>, tx: Sender<TimedRecord>) {
        info!("Added worker: {}", self.workers.len());
        let id = self.next_id;
        let token = CancellationToken::new();
        let worker_token = token.clone();

        let handle = thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let mut log = SegmentedLog::new(&format!("wals/wal_{}", id), 200 * 2048 * 2048)
                    .await
                    .unwrap();
                let mut batch = Vec::with_capacity(100_000);

                let mut delayed = vec![];
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

                                    // todo fix no more values and delayed not empty

                                    if tx.len() > 100_000 {
                                        delayed.extend(batch.drain(..));
                                    }else {
                                        if !delayed.is_empty() {
                                            // empty old
                                            for r in delayed.drain(..) { tx.send(r).unwrap() }
                                        }

                                        for r in batch.drain(..) { tx.send(r).unwrap(); }
                                    }
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
            info!("Remove worker: {}", self.workers.len());
            worker.cancel_token.cancel();
            let _ = worker.handle.join();
            info!("WAL Worker {} removed.", id);
        }
    }
}

pub fn handle_wal_to_engines(
    rt: &Runtimes,
    receiver: Receiver<TimedRecord>,
    incoming_control_rx: Receiver<u64>,
) -> (Receiver<TimedRecord>, Receiver<u64>) {
    let (wal_tx, wal_rx) = unbounded();
    let wal_tx_clone = wal_tx.clone();

    let (_, control_rx_wal) = unbounded();

    rt.attach_runtime(&0, async move {
        log_channel(wal_tx_clone, "WAL -> Engines", None).await;
    });

    thread::spawn(move || {
        let mut manager = WalManager::new();

        // wal logger
        for _ in 0..1 {
            let rx = receiver.clone();
            let tx = wal_tx.clone();
            manager.add_worker(rx, tx);
        }

        let repetition = 3; // logger sends value every second, how many do we wait to increase
        let threshold = 100_000;

        let mut over = 0;
        let mut under = 0;
        loop {
            let res: u64 = incoming_control_rx.recv().unwrap();
            if res > threshold {
                over += 1;
                under = 0;
                if over > repetition {
                    let rx = receiver.clone();
                    let tx = wal_tx.clone();
                    manager.add_worker(rx, tx);
                    over = 0;
                }
            } else if res == 0 && manager.workers() > 1 {
                over = 0;
                under += 1;
                if under > repetition {
                    manager.remove_worker();
                    under = 0;
                }
            }
        }
    });

    (wal_rx, control_rx_wal)
}
