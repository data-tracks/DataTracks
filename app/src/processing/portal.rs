use crate::util::Storage;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tracing::{error, warn};
use value::Time;
use value::train::{Train, TrainId};

pub struct Portal {
    shared_state: Arc<Mutex<SharedState>>,
}

/// Thread-safe implementation for a train saver
impl Portal {
    pub fn new() -> Result<Self, String> {
        Ok(Portal {
            shared_state: Arc::new(Mutex::new(SharedState::new()?)),
        })
    }

    pub fn push(&self, train: Train) {
        let mut state = self.shared_state.lock().unwrap();
        state.push(train);
    }

    pub fn peek(&self, cond: Box<dyn Fn(Time) -> bool>) -> Vec<Train> {
        let state = self.shared_state.lock().unwrap();
        let mut values = vec![];
        for (i, time) in state.timestamps.iter() {
            if cond(*time) {
                match state.storage.read_train(*i) {
                    Some(t) => values.push(t),
                    None => warn!("Error reading train {}", time),
                }
            }
        }
        values
    }
}

impl Clone for Portal {
    fn clone(&self) -> Self {
        Portal {
            shared_state: self.shared_state.clone(),
        }
    }
}

struct SharedState {
    timestamps: BTreeMap<TrainId, Time>,
    storage: Storage,
}

impl SharedState {
    fn new() -> Result<Self, String> {
        let database = Storage::new_temp().unwrap();
        Ok(SharedState {
            timestamps: Default::default(),
            storage: database,
        })
    }

    fn push(&mut self, train: Train) {
        let time = train.event_time;
        let id = train.id;
        match self.storage.write_train(id, train) {
            Ok(_) => {
                self.timestamps.insert(id, time);
            }
            Err(err) => error!("{:?}", err),
        }
    }
}
