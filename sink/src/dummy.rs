use std::time::Duration;
use util::queue::{Meta, RecordContext, RecordQueue};
use value::Value;

pub struct DummySink {
    value: Value,
    interval: Duration,
}

impl DummySink {
    pub fn new(value: Value, interval: Duration) -> Self {
        DummySink { value, interval }
    }

    pub async fn start(&mut self, name: String, value_queue: RecordQueue) {
        let duration = self.interval.clone();
        let value = self.value.clone();
        loop {
            value_queue
                .push(
                    value.clone(),
                    RecordContext::new(Meta::new(Some(name.clone())), name.clone()),
                )
                .await
                .unwrap();
            tokio::time::sleep(duration).await;
        }
    }
}
