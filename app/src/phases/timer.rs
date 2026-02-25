use flume::{Receiver, Sender, unbounded};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use util::Event::Heartbeat;
use util::{InitialRecord, Runtimes, TimedMeta, TimedRecord, get_statistic_sender, log_channel};

struct TimerWorker {
    handle: JoinHandle<()>,
    cancel_token: CancellationToken,
}

pub struct TimerManager {
    workers: Vec<(u64, TimerWorker)>,
    next_id: u64,
    counter: Arc<AtomicU64>,
}

impl TimerManager {
    fn new() -> Self {
        let counter = Arc::new(AtomicU64::new(0));
        Self {
            workers: vec![],
            next_id: 0,
            counter,
        }
    }

    pub(crate) fn workers(&self) -> usize {
        self.workers.len()
    }

    pub fn add_worker(&mut self, incoming: Receiver<InitialRecord>, sender: Sender<TimedRecord>) {
        info!("Added worker: {}", self.workers.len());

        const BATCH_SIZE: u64 = 1_000_000;
        let id = self.next_id;
        let token = CancellationToken::new();
        let token_clone = token.clone();

        let timer_workers = 4;

        let id_source = self.counter.clone();

        let handle = thread::spawn(move || {
            let rt_timer = Builder::new_multi_thread()
                .worker_threads(4)
                .thread_name(format!("timer-processor-{}", id))
                .enable_all()
                .build()
                .unwrap();

            // timer
            let incoming = incoming.clone();
            let sender = sender.clone();

            let mut joins: JoinSet<()> = JoinSet::new();

            rt_timer.block_on(async move {
                for i in 0..timer_workers {
                    let incoming = incoming.clone();
                    let sender = sender.clone();
                    let id_source = id_source.clone();
                    let worker_token = token_clone.clone();

                    joins.spawn(async move {
                        let statistics_sender = get_statistic_sender().unwrap();
                        let name = format!("Timer {} {}", id, i);

                        let mut current_id = id_source.fetch_add(BATCH_SIZE, Ordering::Relaxed);
                        let mut end_id = current_id + BATCH_SIZE;

                        let mut hb_ticker = tokio::time::interval(Duration::from_secs(5));
                        let heartbeat_name = name.clone();

                        loop {
                            tokio::select! {
                                // Priority: Check cancellation first
                                _ = worker_token.cancelled() => {
                                    info!("WAL Worker {} shutting down gracefully", id);
                                    return;
                                }

                                // Timer-based Heartbeat: Fires even if no data is being received
                                _ = hb_ticker.tick() => {
                                    let _ = statistics_sender.send(Heartbeat(heartbeat_name.clone()));
                                }

                                // Data Processing
                                res = incoming.recv_async() => {
                                    match res {
                                        Ok(InitialRecord { value, meta }) => {
                                            // Check if we need a new ID block
                                            if current_id >= end_id {
                                                current_id = id_source.fetch_add(BATCH_SIZE, Ordering::Relaxed);
                                                end_id = current_id + BATCH_SIZE;
                                            }

                                            let id = current_id;
                                            current_id += 1;

                                            let context = TimedMeta::new(id, meta);

                                            if let Err(err) = sender.send((value, context).into()) {
                                                error!("Worker {} failed to send downstream: {}", id, err);
                                                // If downstream is closed, we should probably stop
                                                return;
                                            }
                                        }
                                        Err(_) => {
                                            // Channel closed
                                            error!("Incoming channel closed for worker {}", i);
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    });
                    // to distribute the "workers"
                    sleep(Duration::from_millis(50)).await;
                }
                joins.join_all().await;
            });
            info!("finished all");
        });

        self.workers.push((
            id,
            TimerWorker {
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
            info!("Timer worker {} removed.", id);
        }
    }
}

pub fn handle_initial_time_annotation(
    incoming: Receiver<InitialRecord>,
    rt: &Runtimes,
    sender: Sender<TimedRecord>,
    control_rx: Receiver<u64>,
) -> Receiver<u64> {
    let (control_tx_timer, control_rx_timer) = unbounded();

    let sender_clone = sender.clone();
    rt.attach_runtime(&0, async move {
        log_channel(
            sender_clone,
            "Time Annotation -> WAL",
            Some(control_tx_timer),
        )
        .await;
    });

    thread::spawn(move || {
        let mut manager = TimerManager::new();

        // wal logger
        for _ in 0..1 {
            let rx = incoming.clone();
            let tx = sender.clone();
            manager.add_worker(rx, tx);
        }

        let repetition = 3; // logger sends update every second how long do we wait
        let threshold = 100_000;

        let mut over = 0;
        let mut under = 0;
        loop {
            let res: u64 = control_rx.recv().unwrap();
            if res > threshold {
                over += 1;
                under = 0;
                if over > repetition {
                    let rx = incoming.clone();
                    let tx = sender.clone();
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
    control_rx_timer
}
