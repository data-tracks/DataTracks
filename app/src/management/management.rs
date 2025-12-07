use crate::management::storage::Storage;
use crate::processing::{Plan, Train};
use crate::tpc::start_tpc;
use crate::ui::start_web;
use reqwest::blocking::Client;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{error, info};
use engine::Engine;
use value::Time;

#[derive(Default)]
pub struct Manager {
    storage: Storage,
    engines: HashMap<usize, Engine>,
    joins: JoinSet<()>,
}

impl Manager {
    pub fn new() -> Manager {
        Manager {
            joins: JoinSet::new(),
            storage: Storage::default(),
            engines: HashMap::new(),
        }
    }

    fn get_storage(&self) -> Storage {
        self.storage.clone()
    }

    pub async fn start(mut self) {
        let ctrl_c_signal = tokio::signal::ctrl_c();

        let mut join_set: JoinSet<()> = JoinSet::new();

        //add_default(self.get_storage());

        self.start_services();

        for (name, engine) in Engine::start_all().await.unwrap().into_iter().enumerate() {
            self.engines.insert(name, engine);
        }


        tokio::select! {
                _ = ctrl_c_signal => {
                    info!("#️⃣ Ctrl-C received!");
                }
                Some(res) = join_set.join_next() => {
                    if let Err(e) = res {
                        error!("\nFatal Error: A core task crashed: {:?}", e);
                    }
                }
        }


        for (_, mut e) in self.engines.drain() {
            e.stop().await.unwrap();
        }

        // Clean up all remaining running tasks
        info!("🧹 Aborting remaining tasks...");
        join_set.abort_all();
        while join_set.join_next().await.is_some() {}

        info!("✅  All services shut down. Exiting.");
    }

    fn start_services(&mut self) {
        let web_storage = self.get_storage();
        let tpc_storage = self.get_storage();

        self.joins.spawn(start_web(web_storage));

        self.joins
            .spawn(start_tpc("localhost".to_string(), 5959, tpc_storage));
    }
}

fn add_producer() {
    loop {
        let client = Client::new();

        let message = "Hello from Rust!";

        let mut train = Train::new_values(vec![message.into()], 0, 0);
        train.event_time = Time::now();

        let train = serde_json::to_string(&train).unwrap_or(message.to_string());

        let response = client
            .post(format!("http://127.0.0.1:{}/data", 5555))
            //.body(train)
            .json(&train)
            .send();

        match response {
            Ok(_) => {}
            Err(err) => error!("{}", err),
        }
        thread::sleep(Duration::from_secs(1));
    }
}
