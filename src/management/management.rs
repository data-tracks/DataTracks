use crate::http::destination::HttpDestination;
use crate::management::storage::Storage;
use crate::mqtt::{MqttDestination, MqttSource};
use crate::processing::destination::Destination;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::{DebugDestination, HttpSource, Plan};
use crate::tpc::{start_tpc, TpcDestination, TpcSource};
use crate::ui::start_web;
use crossbeam::channel::Sender;
use reqwest::blocking::Client;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tracing::{error, info};

pub struct Manager {
    storage: Arc<Mutex<Storage>>,
    handles: Vec<thread::JoinHandle<()>>,
    server: Option<Sender<Command>>,
}

impl Manager {
    pub fn new() -> Manager {
        Manager {
            storage: Arc::new(Mutex::new(Storage::new())),
            handles: vec![],
            server: None,
        }
    }

    fn get_storage(&self) -> Arc<Mutex<Storage>> {
        self.storage.clone()
    }

    pub fn start(&mut self) {
        add_default(self.get_storage());

        let web_storage = self.get_storage();
        let tpc_storage = self.get_storage().clone();

        let handle = match thread::Builder::new().name("HTTP Interface".to_string()).spawn(|| start_web(web_storage)) {
            Ok(handle) => handle,
            Err(err) => panic!("Failed to start HTTP Interface {}", err),
        };
        self.handles.push(handle);
        let handle = match thread::Builder::new().name("TPC Interface".to_string()).spawn(|| start_tpc("localhost".to_string(), 5959, tpc_storage)) {
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
                info!("Waiting for thread to finish...");
            }

            match handle.join() {
                Ok(_) => info!("Thread joined successfully."),
                Err(err) => error!("Thread panicked: {:?}", err),
            }
        }
    }
}

fn add_default(storage: Arc<Mutex<Storage>>) {
    let res = thread::Builder::new().name("Default Plan".to_string()).spawn(move || {
        let mut plan = Plan::parse("1--2{sql|SELECT $1 FROM $1}--3").unwrap();

        let source = Box::new(HttpSource::new(String::from("127.0.0.1"), 5555));
        let source_id = source.id();
        plan.add_source(source);
        plan.connect_in_out(1, source_id);

        let source = Box::new(MqttSource::new(String::from("127.0.0.1"), 6666));
        let source_id = source.id();
        plan.add_source(source);
        plan.connect_in_out(1, source_id);

        let source = Box::new(TpcSource::new(String::from("127.0.0.1"), 9999));
        let source_id = source.id();
        plan.add_source(source);
        plan.connect_in_out(1, source_id);

        let destination = Box::new(DebugDestination::new());
        let destination_id = destination.get_id();
        plan.add_destination(destination);
        plan.connect_in_out(3, destination_id);

        let destination = Box::new(MqttDestination::new(String::from("127.0.0.1"), 8888));
        let destination_id = destination.get_id();
        plan.add_destination(destination);
        plan.connect_in_out(3, destination_id);

        let destination = Box::new(TpcDestination::new(String::from("127.0.0.1"), 8686));
        let destination_id = destination.get_id();
        plan.add_destination(destination);
        plan.connect_in_out(3, destination_id);

        let destination = Box::new(HttpDestination::new(String::from("127.0.0.1"), 9696));
        let destination_id = destination.get_id();
        plan.add_destination(destination);
        plan.connect_in_out(3, destination_id);

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

        let response = client
            .post(format!("http://127.0.0.1:{}/data", 5555))
            .json(&message)
            .send();

        match response {
            Ok(_) => {}
            Err(err) => error!("{}", err),
        }
        thread::sleep(Duration::from_secs(1));
    }
}
