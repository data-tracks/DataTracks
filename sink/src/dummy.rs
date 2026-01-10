use flume::{Sender};
use std::time::Duration;
use tracing::error;
use util::InitialMeta;
use value::Value;

pub struct DummySink {
    value: Value,
    interval: Duration,
}

impl DummySink {
    pub fn new(value: Value, interval: Duration) -> Self {
        DummySink { value, interval }
    }

    pub async fn start(&mut self, name: String, sender: Sender<(Value, InitialMeta)>) {
        let duration = self.interval;
        let value = self.value.clone();
        loop {
            match sender
                .send((value.clone(), InitialMeta::new(Some(name.clone())))) {
                Ok(_) => {}
                Err(err) => {
                    error!("Could not sink: {}", err)
                }
            }
            tokio::time::sleep(duration).await;
        }
    }
}
