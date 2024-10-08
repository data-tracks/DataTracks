use crate::management::storage::Storage;
use crate::mqtt::MqttSource;
use crate::processing::{DebugDestination, HttpSource, Plan};
use crate::ui::start_web;
use std::sync::{Arc, Mutex};
use std::thread;
use tracing::info;

pub struct Manager {
    storage: Arc<Mutex<Storage>>,
    handles: Vec<thread::JoinHandle<()>>,
}


impl Manager {
    pub fn new() -> Manager {
        Manager { storage: Arc::new(Mutex::new(Storage::new())), handles: vec![] }
    }

    fn get_storage(&self) -> Arc<Mutex<Storage>> {
        self.storage.clone()
    }

    pub fn start(&mut self) {
        add_default(self.get_storage());

        let web_storage = self.get_storage();

        let handle = thread::spawn(|| start_web(web_storage));
        self.handles.push(handle);
    }

    pub fn shutdown(&mut self) {
        for handle in self.handles.drain(..) {
            if handle.is_finished() {
                info!("Thread finished.");
                handle.join().unwrap();
            }
        }
    }
}

fn add_default(storage: Arc<Mutex<Storage>>) {
    thread::spawn(move || {
        let mut plan = Plan::parse("1-2{sql|SELECT $1 FROM $1}-3").unwrap();

        plan.add_source(1, Box::new(HttpSource::new(1, String::from("127.0.0.1"), 5555)));
        plan.add_source(2, Box::new(MqttSource::new(2, String::from("127.0.0.1"), 6666)));
        plan.add_destination(3, Box::new(DebugDestination::new(3)));
        let id = plan.id;
        plan.set_name("Default".to_string());
        let mut lock = storage.lock().unwrap();
        lock.add_plan(plan);
        lock.start_plan(id);
        drop(lock);
    });
}

