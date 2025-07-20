use crate::management::storage::Storage;
use crate::processing::{Plan, Train};
use crate::tpc::start_tpc;
use crate::ui::start_web;
use reqwest::blocking::Client;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tracing::{error, info};
use value::Time;

#[derive(Default)]
pub struct Manager {
    storage: Arc<Mutex<Storage>>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl Manager {
    pub fn new() -> Manager {
        Manager {
            storage: Arc::new(Mutex::new(Storage::new())),
            handles: vec![],
        }
    }

    fn get_storage(&self) -> Arc<Mutex<Storage>> {
        self.storage.clone()
    }

    pub fn start(&mut self) {
        add_default(self.get_storage());

        let web_storage = self.get_storage();
        let tpc_storage = self.get_storage().clone();

        let handle = match thread::Builder::new()
            .name("HTTP Interface".to_string())
            .spawn(|| start_web(web_storage))
        {
            Ok(handle) => handle,
            Err(err) => panic!("Failed to start HTTP Interface {}", err),
        };
        self.handles.push(handle);
        let handle = match thread::Builder::new()
            .name("TPC Interface".to_string())
            .spawn(|| start_tpc("localhost".to_string(), 5959, tpc_storage))
        {
            Ok(handle) => handle,
            Err(err) => panic!("Failed to start TPC Interface {}", err),
        };
        self.handles.push(handle);
    }

    pub fn shutdown(&mut self) {
        for handle in self.handles.drain(..) {
            if handle.is_finished() {
                info!("Thread finished.");
            } else {
                info!(
                    "Waiting for thread to finish {:?}...",
                    handle.thread().name().unwrap_or("unknown")
                );
            }

            match handle.join() {
                Ok(_) => info!("Thread joined successfully."),
                Err(err) => error!("Thread panicked: {:?}", err),
            }
        }
    }
}

fn add_default(storage: Arc<Mutex<Storage>>) {
    let res = thread::Builder::new()
        .name("Default Plan".to_string())
        .spawn(move || {
            let mut plan = Plan::parse(
                "\
                1--2{sql|SELECT $1 FROM $1}[2s]--3\n\
                In\n\
                HTTP{\"url\":\"127.0.0.1\", \"port\": \"5555\"}:1\n\
                MQTT{\"url\":\"127.0.0.1\", \"port\": 6666}:1\n\
                TPC{\"url\":\"127.0.0.1\", \"port\": 9999}:1\n\
                Out\n\
                MQTT{\"url\":\"127.0.0.1\", \"port\": 8888}:3\n\
                TPC{\"url\":\"127.0.0.1\", \"port\": 8686}:3\n\
                HTTP{\"url\":\"127.0.0.1\", \"port\": 9696}:3",
            )
            .unwrap();

            let id = plan.id;
            plan.set_name("Default".to_string());
            let mut lock = storage.lock().unwrap();
            lock.add_plan(plan);
            lock.start_plan(id);
            drop(lock);

            add_producer();
        });

    match res {
        Ok(_) => {}
        Err(err) => error!("{}", err),
    }
}

fn add_producer() {
    loop {
        let client = Client::new();

        let message = "Hello from Rust!";

        let mut train = Train::new(vec![message.into()], 0);
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
