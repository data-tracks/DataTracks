use flume::Sender;
use std::time::Duration;
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
            if sender
                .send((value.clone(), InitialMeta::new(Some(name.clone())))).is_ok() {};
            tokio::time::sleep(duration).await;
        }
    }
}
