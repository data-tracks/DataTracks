use flume::{Receiver, Sender, unbounded};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
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
    id_queue: Receiver<Vec<u64>>,
    rt_timer_manager: Runtime,
}

impl TimerManager {
    fn new() -> Self {
        let rt_timer_manager = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("timer-processor")
            .enable_all()
            .build()
            .unwrap();

        let (tx, id_queue) = unbounded();

        rt_timer_manager.spawn(async { start_id_generator(tx, 2).await });

        Self {
            workers: vec![],
            next_id: 0,
            rt_timer_manager,
            id_queue,
        }
    }

    pub(crate) fn workers(&self) -> usize {
        self.workers.len()
    }

    pub fn add_worker(&mut self, incoming: Receiver<InitialRecord>, sender: Sender<TimedRecord>) {
        info!("Added worker: {}", self.workers.len());
        let id = self.next_id;
        let token = CancellationToken::new();
        let token_clone = token.clone();

        let timer_workers = 4;
        let id_queue = self.id_queue.clone();

        let handle = thread::spawn(move || {
            let rt_timer = Builder::new_current_thread()
                .thread_name(format!("timer-processor-{}", id))
                .enable_all()
                .build()
                .unwrap();

            // timer
            let incoming = incoming.clone();
            let sender = sender.clone();
            let id_queue = id_queue.clone();

            let mut joins: JoinSet<()> = JoinSet::new();

            rt_timer.block_on(async move {
                for i in 0..timer_workers {
                    let incoming = incoming.clone();
                    let sender = sender.clone();
                    let id_queue = id_queue.clone();
                    let worker_token = token_clone.clone();

                    joins.spawn(async move {
                        let mut available_ids = vec![];
                        let statistics_sender = get_statistic_sender();
                        let name = format!("Timer {} {}", id, i);
                        loop {
                            statistics_sender.send(Heartbeat(name.clone())).unwrap();
                            if worker_token.is_cancelled() {
                                info!("WAL Worker {} shutting down gracefully", id);
                                return;
                            }

                            //info!("else {}", id);
                            if available_ids.is_empty() {
                                match id_queue.recv_async().await {
                                    Ok(ids) => available_ids.extend(ids),
                                    Err(_) => {
                                        error!("No available ids in worker {}", i);
                                        sleep(Duration::from_millis(50)).await;
                                        continue;
                                    }
                                }
                            }
                            if available_ids.is_empty() {
                                error!("No available ids in worker {}", i);
                                sleep(Duration::from_millis(50)).await;
                                continue;
                            }

                            match incoming.recv_async().await {
                                Err(_) => {
                                    error!("No incoming {}", i);
                                }
                                Ok(InitialRecord { value, meta }) => {
                                    let id = available_ids.pop().unwrap(); // can unwrap, check above
                                    let context = TimedMeta::new(id, meta);
                                    sender.send((value, context).into()).unwrap();
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

        let repetition = 10;
        let threshold = 10_000;

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

async fn start_id_generator(tx: Sender<Vec<u64>>, workers: i32) {
    log_channel(tx.clone(), "Id generator", None).await;

    let prepared_ids = (workers * 2) as usize;
    const ID_PACKETS_SIZE: u64 = 100_000;

    // we prepare as much "id packets" as we have workers plus some more
    let mut count = 0u64;
    loop {
        if tx.len() > prepared_ids {
            sleep(Duration::from_millis(50)).await;
        } else {
            let mut ids: Vec<u64> = vec![ID_PACKETS_SIZE];
            for _ in 0..ID_PACKETS_SIZE {
                ids.push(count);
                count += 1;
            }
            tx.send(ids).unwrap();
        }
    }
}
