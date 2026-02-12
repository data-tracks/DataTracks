use async_trait::async_trait;
use flume::{Sender, unbounded};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::runtime::{Builder, Runtime};

struct Runner {
    runtime: Runtime,
    sender: Sender<u64>,
    blueprint: Box<dyn Task>,
}

pub struct RuntimeManager {
    runtime: Runtime,
    runtimes: HashMap<u64, Runner>,
    id_generator: AtomicU64,
}

impl RuntimeManager {
    pub(crate) fn add_runtime(&mut self, runtime: Runtime, blueprint: Box<dyn Task>) -> u64 {
        let id = self.id_generator.fetch_add(1, Ordering::Relaxed);

        let (tx, _) = unbounded();

        let runner = Runner {
            runtime,
            sender: tx,
            blueprint,
        };
        self.runtimes.insert(id, runner);
        id
    }

    pub fn new() -> Self {
        Self {
            runtime: Builder::new_current_thread()
                .thread_name("rt-manager")
                .build()
                .unwrap(),
            runtimes: HashMap::new(),
            id_generator: AtomicU64::new(0),
        }
    }
}

impl Default for RuntimeManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
trait Task: Send + Sync + 'static {
    async fn run(self, sender: Sender<u64>) -> anyhow::Result<()>;

    fn clone_box(&self) -> Box<dyn Task>;
}

impl Clone for Box<dyn Task> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[cfg(test)]
mod test {
    use crate::{RuntimeManager, Task};
    use async_trait::async_trait;
    use flume::Sender;
    use std::time::Duration;
    use tokio::runtime::Builder;
    use tokio::time::sleep;

    pub struct TestRunner {}

    #[async_trait]
    impl Task for TestRunner {
        async fn run(self, sender: Sender<u64>) -> anyhow::Result<()> {
            sleep(Duration::from_secs(5)).await;
            sender.send_async(100).await?;
            loop {
                sleep(Duration::from_secs(5)).await
            }
        }

        fn clone_box(&self) -> Box<dyn Task> {
            Box::new(TestRunner {})
        }
    }

    #[test]
    pub fn test_manager() {
        let mut manager = RuntimeManager::default();

        manager.add_runtime(
            Builder::new_current_thread().build().unwrap(),
            Box::new(TestRunner {}),
        );
    }
}
