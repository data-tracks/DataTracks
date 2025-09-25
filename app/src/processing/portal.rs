use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tracing::error;
use value::train::{Train, TrainId};
use value::Time;
use core::Storage;
use error::error::TrackError;

pub struct Portal {
    shared_state: Arc<Mutex<SharedState>>,
}

/// Thread-safe implementation for a train saver
impl Portal {
    pub fn new() -> Result<Self, TrackError> {
        Ok(Portal {
            shared_state: Arc::new(Mutex::new(SharedState::new()?)),
        })
    }

    pub fn push_trains(&self, trains: Vec<Train>) {
        let mut state = self.shared_state.lock().unwrap();
        state.push_trains(trains);
    }

    pub fn push_train(&self, train: Train) {
        let mut state = self.shared_state.lock().unwrap();
        state.push_train(train);
    }

    pub fn peek(&self) -> BTreeMap<TrainId, Time> {
        let state = self.shared_state.lock().unwrap();
        state.timestamps.clone()
    }

    pub fn get_train(&self, train_id: TrainId) -> Option<Train> {
        let state = self.shared_state.lock().unwrap();
        state.storage.read_train(train_id)
    }

    pub fn get_trains(&self, ids: Vec<TrainId>) -> Vec<Train> {
        let mut trains = vec![];

        let state = self.shared_state.lock().unwrap();
        for id in ids {
            match state.storage.read_train(id) {
                None => {}
                Some(t) => trains.push(t),
            }
        }
        trains
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
        let database = Storage::new_temp()?;
        Ok(SharedState {
            timestamps: Default::default(),
            storage: database,
        })
    }

    fn push_trains(&mut self, trains: Vec<Train>) {
        let ids = trains
            .iter()
            .map(|t| (t.id, t.event_time))
            .collect::<Vec<_>>();
        match self.storage.write_trains(trains) {
            Ok(_) => {
                for (id, time) in ids {
                    self.timestamps.insert(id, time);
                }
            }
            Err(err) => error!("{:?}", err),
        }
    }

    fn push_train(&mut self, train: Train) {
        let id = train.id;
        let time = train.event_time;
        match self.storage.write_train(id, train) {
            Ok(_) => {
                self.timestamps.insert(id, time);
            }
            Err(err) => error!("{:?}", err),
        }
    }
}
