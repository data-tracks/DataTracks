use std::collections::VecDeque;
use flume::{unbounded, Receiver, Sender};
use std::{cmp, thread};
use std::time::Duration;
use futures::pin_mut;
use tokio::runtime::Builder;
use tokio::time;
use tokio::time::{interval, Instant};
use tokio_util::sync::CancellationToken;
use tracing::info;
use util::{log_channel, Event, QueueEvent, Runtimes, SegmentedLog, TimedRecord};

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

    pub fn add_worker(&mut self, rx: Receiver<TimedRecord>, tx: Sender<TimedRecord>, statistics: Sender<Event>) {
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

                let mut delayed = VecDeque::new();

                let duration = Duration::from_millis(500);
                let long_duration = Duration::from_secs(60 * 60 * 24);
                let timer = time::sleep(long_duration);
                pin_mut!(timer);
                let mut timer_active = false;

                let name = format!("WAL Delayed {}", id);

                let mut delay_hb = interval(Duration::from_secs(3));

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

                                    if tx.len() >= 200_000 {
                                        delayed.extend(batch.drain(..));
                                        timer.as_mut().reset(Instant::now() + duration);
                                        timer_active = true;
                                    }else {
                                        if !delayed.is_empty() {
                                            // empty old
                                            let count = cmp::min(delayed.len(), 100_000);
                                            for r in delayed.drain(0..count) { tx.send(r).unwrap() }

                                            if !delayed.is_empty(){
                                                // still not empty
                                                delayed.extend(batch.drain(..));
                                                timer.as_mut().reset(Instant::now() + duration);
                                                timer_active = true;
                                            }else {
                                                for r in batch.drain(..) { tx.send(r).unwrap(); }
                                            }
                                        }else {
                                            for r in batch.drain(..) { tx.send(r).unwrap(); }
                                        }


                                    }
                                }
                                Err(_) => return, // Channel closed
                            }
                        }
                        () = &mut timer, if timer_active && tx.len() <= 100_000 => {
                            // empty old
                            info!("writing buffered messages len: {}", delayed.len());
                            let count = cmp::min(delayed.len(), 100_000);
                            for r in delayed.drain(0..count) { tx.send(r).unwrap() }
                            timer.as_mut().reset(Instant::now() + long_duration);
                            timer_active = false;
                        }
                        _ = delay_hb.tick() => {
                            statistics.send_async(Event::Queue (QueueEvent{name: name.clone(), size: delayed.len()})).await.unwrap();
                            statistics.send_async(Event::Heartbeat(name.clone())).await.unwrap();
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
    statistics: Sender<Event>,
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
            manager.add_worker(rx, tx, statistics.clone());
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
                    manager.add_worker(rx, tx, statistics.clone());
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
