use crate::processing::destination::Destinations;
use crate::processing::ledger::Ledger;
use crate::processing::source::Sources;
use crate::processing::transform::Transforms;
use crate::util::Tx;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot::Sender;
use value::Time;

type SourceStorage = Mutex<HashMap<String, fn(Map<String, Value>) -> Sources>>;
type DestinationStorage = Mutex<HashMap<String, fn(Map<String, Value>) -> Destinations>>;
type TransformStorage = Mutex<HashMap<String, fn(Map<String, Value>) -> Transforms>>;

#[derive(Default)]
pub struct Storage {
    link: Arc<Mutex<State>>,
}
impl Clone for Storage {
    fn clone(&self) -> Self {
        Storage {
            link: self.link.clone(),
        }
    }
}

#[derive(Default)]
struct State {
    pub plans: HashMap<usize, Ledger>,
}

pub struct Attachment {
    data_port: u16,
    watermark_port: u16,
    wm_sender: Tx<Time>,
    shutdown_channel: Sender<bool>,
    wm_shutdown_channel: Sender<bool>,
}

impl Attachment {
    pub fn new(
        data_port: u16,
        watermark_port: u16,
        wm_sender: Tx<Time>,
        shutdown_channel: Sender<bool>,
        wm_shutdown_channel: Sender<bool>,
    ) -> Self {
        Attachment {
            watermark_port,
            wm_sender,
            data_port,
            wm_shutdown_channel,
            shutdown_channel,
        }
    }
}

impl Storage {

    pub fn add_plan(&mut self, plan: Ledger) -> Result<(), String> {
        let mut state = self.link.lock().unwrap();

        let id = plan.id;
        state
            .plans
            .insert(plan.id, plan)
            .ok_or(format!("No plan with id {id}"))
            .map(|_| ())
    }

    pub fn delete_plan(&mut self, id: usize) -> Result<(), String> {
        let mut state = self.link.lock().unwrap();

        state
            .plans
            .remove(&id)
            .ok_or(format!("No plan with id {id}"))
            .map(|_| ())
    }
}
