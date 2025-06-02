use crate::processing;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use value::train::Train;
use value::Time;

pub type Storage = Arc<Mutex<Vec<Train>>>;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Copy)]
pub struct WindowDescriptor {
    from: Time,
    to: Time,
    unbounded: bool,
}

impl WindowDescriptor {
    pub fn new(from: Time, to: Time) -> WindowDescriptor {
        WindowDescriptor {from, to, unbounded: false}
    }

    pub fn unbounded() -> WindowDescriptor {
        WindowDescriptor {from: Time::now(), to: Time::now(), unbounded: true}
    }
}


pub struct WindowSelector {
    storage: Arc<Storage>,
    window: processing::window::Window,
    strategy: Box<dyn FnMut(&Time) -> Vec<(WindowDescriptor, bool)>>
}

impl WindowSelector {
    pub(crate) fn new(storage: Arc<Storage>, window: processing::window::Window) -> Self {
        let strategy = window.get_strategy();
        Self { storage, window, strategy }
    }
}

impl WindowSelector {
    pub(crate) fn select(&self, current: &Time) -> Vec<(WindowDescriptor, bool)> {
        self.strategy(current)
    }
}

pub struct TriggerSelector {
    triggered_windows: HashMap<WindowDescriptor, TriggerStatus>,
    storage: Arc<Storage>,
    fire_early: bool,
    fire_late: bool,
    re_fire: bool,
}


impl TriggerSelector {

    pub(crate) fn new(storage: Arc<Storage>) -> Self {
        TriggerSelector{
            triggered_windows: Default::default(),
            storage,
            fire_early: false,
            fire_late: false,
            re_fire: false,
        }
    }

    pub(crate) fn select(&mut self, windows: Vec<(WindowDescriptor, bool)>) -> Vec<Train> {
        let mut trains = vec![];
        windows.into_iter().for_each(|(window, is_complete)| {
            let mut trigger = false;
            if is_complete {
                trigger = true;
                self.triggered_windows.insert(window, TriggerStatus::OnTime);
            } else if let Some(status) = self.triggered_windows.get(&window) {
                if self.re_fire && status == &TriggerStatus::Early  {
                    trigger = true;
                }else if self.fire_late && status == &TriggerStatus::OnTime {
                    trigger = true;
                }
            }
            if trigger {
                match self.get_trains(window) {
                    None => {}
                    Some(t) => trains.push(t),
                }
            }
        });
        trains
    }
    fn get_trains(&self, window: WindowDescriptor) -> Option<Train> {
        let storage = self.storage.lock().unwrap();
        storage
            .iter()
            .filter(|train| window.from <= train.event_time && window.to >= train.event_time  )
            .map(|train| train.clone())
            .reduce(|a,b| {
                a + b
            })
    }
}



#[derive(Clone,PartialEq)]
pub enum TriggerStatus {
    Early,
    OnTime,
}
