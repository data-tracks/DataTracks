use flume::Sender;
use std::time::Duration;
use tracing::error;
use util::Event::Heartbeat;
use util::{Event, InitialMeta, InitialRecord};
use value::Value;

pub enum DummySink {
    Interval {
        value: Value,
        interval: Duration,
    },
    Ramping {
        value: Value,
        interval: Duration,
        delta: Duration,
    },
}

impl DummySink {
    pub fn interval(value: Value, interval: Duration) -> Self {
        DummySink::Interval { value, interval }
    }

    pub async fn start(
        &mut self,
        id: usize,
        name: String,
        sender: Sender<InitialRecord>,
        statistics_tx: Sender<Event>,
    ) {
        match self {
            DummySink::Interval { value, interval } => {
                let heartbeat_id = format!("DummyInterval {} {}", name, id);
                let meta_name = Some(name.clone());

                let mut data_ticker = tokio::time::interval(*interval);
                // Heartbeat every 5 seconds
                let mut hb_ticker = tokio::time::interval(Duration::from_secs(3));

                loop {
                    tokio::select! {
                        // Case 1: Time to send data
                        _ = data_ticker.tick() => {
                            let record = (value.clone(), InitialMeta::new(meta_name.clone())).into();
                            if let Err(err) = sender.send(record) {
                                error!("Could not sink: {}", err);
                            }
                        }

                        // Case 2: Time to send heartbeat (much less frequent)
                        _ = hb_ticker.tick() => {
                            let _ = statistics_tx.send(Heartbeat(heartbeat_id.clone()));
                        }
                    }
                }
            }
            DummySink::Ramping {
                value,
                interval,
                delta,
            } => {
                let heartbeat_id = format!("DummyRamping {} {}", name, id);
                let meta_name = Some(name.clone());

                // 1. Setup heartbeat timer (e.g., every 5 seconds)
                let mut hb_ticker = tokio::time::interval(Duration::from_secs(3));

                // 2. Track the dynamic data interval
                let mut current_interval = *interval;
                let mut next_send = tokio::time::Instant::now();

                loop {
                    tokio::select! {
                        _ = hb_ticker.tick() => {
                            let _ = statistics_tx.send(Heartbeat(heartbeat_id.clone()));
                        }

                        _ = tokio::time::sleep_until(next_send) => {
                            let record = (value.clone(), InitialMeta::new(meta_name.clone())).into();

                            if let Err(err) = sender.send(record) {
                                error!("Could not sink ({}): {}", heartbeat_id, err);
                            }

                            // Update the interval and schedule the next send
                            current_interval = current_interval.saturating_add(*delta);
                            next_send = tokio::time::Instant::now() + current_interval;
                        }
                    }
                }
            }
        }
    }
}
