use flume::{Receiver, Sender, bounded, unbounded};
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::info;
use util::{
    Event, QueueEvent, Runtimes, SegmentedIndex, SegmentedLogWriter, TimedRecord, log_channel,
};

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

    pub fn add_worker(
        &mut self,
        rx: Receiver<Vec<TimedRecord>>,
        tx: Sender<Vec<TimedRecord>>,
        statistics: Sender<Event>,
    ) {
        info!("Added worker: {}", self.workers.len());
        let id = self.next_id;
        let token = CancellationToken::new();
        let worker_token = token.clone();

        let (seg_id_tx, seg_id_rx) = unbounded::<(u64, u64, SegmentedIndex)>();
        let (buff_tx, buff_rx) = bounded(100_000);
        let buff_token = token.clone();

        let handle = thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let mut log = SegmentedLogWriter::new(&format!("temp/wals/wal_{}", id))
                    .await
                    .unwrap();

                for _ in 0..4 {
                    let reader = log.build_reader().await.unwrap();
                    let buff_token = buff_token.clone();
                    let seg_id_rx = seg_id_rx.clone();
                    let buff_tx = buff_tx.clone();

                    thread::spawn(move || {
                        let rt = Builder::new_current_thread().enable_all().build().unwrap();

                        rt.block_on(async {
                            loop {
                                tokio::select! {
                                    biased;
                                    _ = buff_token.cancelled() => {
                                        info!("WAL Buffer {} shutting down gracefully", id);
                                        return;
                                    }
                                    index = seg_id_rx.recv_async() => match index {
                                            Ok(index) => {
                                                let record = reader.unlog(&index.2).await;
                                                // as long as it is not emptied we wait here
                                                let _ = buff_tx.send(record);
                                            }
                                            Err(_) => return, // Channel closed
                                        }
                                }
                            }
                        });
                    });
                }

                let mut batch = Vec::with_capacity(100_000);

                let mut delayed_length = 0usize;

                let mut duration = interval(Duration::from_millis(50));
                duration.set_missed_tick_behavior(MissedTickBehavior::Skip);

                let name = format!("WAL Delayed {}", id);

                let mut delay_hb = interval(Duration::from_secs(3));

                loop {
                    tokio::select! {
                        biased;
                        // EXIT SIGNAL: The manager called for a shrink
                        _ = worker_token.cancelled() => {
                            info!("WAL Worker {} shutting down gracefully", id);
                            return;
                        }
                        _ = duration.tick() => {
                            let len = tx.len();
                            if !buff_rx.is_empty() && len < 100_000 {
                                // empty old

                                let count = 100_000usize.saturating_sub(buff_rx.len());
                                for records in buff_rx.try_iter().take(count) {
                                    delayed_length -= records.len();
                                    tx.send(records).unwrap()
                                }
                            }

                        }
                        _ = delay_hb.tick() => {
                            statistics.send_async(Event::Queue (QueueEvent{name: name.clone(), size: delayed_length})).await.unwrap();
                            statistics.send_async(Event::Heartbeat(name.clone())).await.unwrap();
                        }

                        // WORK LOGIC
                        res = rx.recv_async() => {
                            match res {
                                Ok(record) => {
                                    batch.extend(record);
                                    batch.extend(rx.try_iter().take(100).flatten());
                                    let index = log.log(&batch).await;

                                    if tx.len() >= 200_000 {
                                        batch.clear();
                                        delayed_length += index.1 as usize;
                                        seg_id_tx.send_async(index).await.unwrap();
                                    }else if !buff_rx.is_empty() {
                                            // empty old
                                            let count = 100_000_usize.saturating_sub(buff_rx.len());
                                            for records in buff_rx.try_iter().take(count) {
                                                delayed_length -= records.len();
                                                tx.send(records).unwrap()
                                            }

                                            if !buff_rx.is_empty(){
                                                // still not empty
                                                batch.clear();
                                                delayed_length += index.1 as usize;
                                                seg_id_tx.send_async(index).await.unwrap();
                                            }else {
                                                tx.send(std::mem::take(&mut batch)).unwrap();
                                            }
                                    }else {
                                        tx.send(std::mem::take(&mut batch)).unwrap();
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
    receiver: Receiver<Vec<TimedRecord>>,
    incoming_control_rx: Receiver<u64>,
    statistics: Sender<Event>,
) -> (Receiver<Vec<TimedRecord>>, Receiver<u64>) {
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
