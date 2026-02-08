use std::ops::{AddAssign};
use flume::{Sender};
use std::time::Duration;
use tracing::error;
use util::InitialMeta;
use value::Value;

pub enum DummySink {
    Interval{
        value: Value,
        interval: Duration,
    },
    Ramping{
        value: Value,
        interval: Duration,
        delta: Duration,
    }
}

impl DummySink {
    pub fn interval(value: Value, interval: Duration) -> Self {
        DummySink::Interval { value, interval }
    }

    pub async fn start(&mut self, name: String, sender: Sender<(Value, InitialMeta)>) {
        match self {
            DummySink::Interval{value,interval} => {
                loop {
                    match sender
                        .send((value.clone(), InitialMeta::new(Some(name.clone())))) {
                        Ok(_) => {}
                        Err(err) => {
                            error!("Could not sink: {}", err)
                        }
                    }
                    tokio::time::sleep(*interval).await;
                }
            }
            DummySink::Ramping { value, interval, delta } => {

                let mut interval = interval.clone();
                let delta = delta.clone();
                loop {
                    match sender
                        .send((value.clone(), InitialMeta::new(Some(name.clone())))) {
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
