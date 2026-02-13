use flume::Sender;
use std::ops::AddAssign;
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
                let id = format!("DummyInterval {} {}", name, id);
                loop {
                    statistics_tx.send(Heartbeat(id.clone())).unwrap();
                    match sender.send((value.clone(), InitialMeta::new(Some(name.clone()))).into())
                    {
                        Ok(_) => {}
                        Err(err) => {
                            error!("Could not sink: {}", err)
                        }
                    }
                    tokio::time::sleep(*interval).await;
                }
            }
            DummySink::Ramping {
                value,
                interval,
                delta,
            } => {
                let id = format!("DummyRamping {} {}", name, id);
                let mut interval = *interval;
                let delta = *delta;
                loop {
                    statistics_tx.send(Heartbeat(id.clone())).unwrap();
                    match sender.send((value.clone(), InitialMeta::new(Some(name.clone()))).into())
                    {
                        Ok(_) => {}
                        Err(err) => {
                            error!("Could not sink: {}", err)
                        }
                    }
                    tokio::time::sleep(interval).await;
                    interval.add_assign(delta);
                }
            }
        }
    }
}
