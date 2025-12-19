use crossbeam::channel::Sender;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use util::queue::{Meta, RecordContext};
use value::Value;

pub struct DummySink {
    value: Value,
    interval: Duration,
}

impl DummySink {
    pub fn new(value: Value, interval: Duration) -> Self {
        DummySink { value, interval }
    }

    pub async fn start(&mut self, name: String, sender: UnboundedSender<(Value, RecordContext)>) {
        let duration = self.interval.clone();
        let value = self.value.clone();
        loop {
            sender
                .send((
                    value.clone(),
                    RecordContext::new(Meta::new(Some(name.clone())), name.clone()),
                ))
                .unwrap();
            tokio::time::sleep(duration).await;
        }
    }
}
