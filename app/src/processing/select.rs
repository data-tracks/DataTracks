use crate::processing;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::info;
use value::train::Train;
use value::Time;
use crate::processing::window::WindowStrategy;

pub type Storage = Arc<Mutex<Vec<Train>>>;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Copy)]
#[derive(Debug)]
pub struct WindowDescriptor {
    from: Time,
    to: Time,
}


impl WindowDescriptor {
    pub fn new(from: Time, to: Time) -> WindowDescriptor {
        WindowDescriptor {from, to}
    }

    pub fn unbounded(time: Time) -> WindowDescriptor {
        WindowDescriptor {from: time.clone(), to: time.clone()}
    }
}


pub struct WindowSelector {
    dirty_windows: HashMap<WindowDescriptor, bool>,
    strategy: WindowStrategy
}

impl WindowSelector {
    pub(crate) fn new(window: processing::window::Window) -> Self {
        let strategy = window.get_strategy();
        Self { dirty_windows: Default::default(), strategy }
    }

    pub(crate) fn mark(&mut self, train: &Train) {
        self.strategy.mark(train).into_iter().for_each(|window| {
            self.dirty_windows.insert(window.0, window.1);
        })
    }
    
    pub(crate) fn select(&mut self) -> HashMap<WindowDescriptor, bool>{
        self.dirty_windows.drain().collect()
    }

}


pub struct TriggerSelector {
    triggered_windows: HashMap<WindowDescriptor, TriggerStatus>,
    storage: Arc<Storage>,
    fire_on: TriggerType,
    fire_early: bool,
    fire_late: bool,
    re_fire: bool,
}


impl TriggerSelector {

    pub(crate) fn new(storage: Arc<Storage>) -> Self {
        TriggerSelector{
            triggered_windows: Default::default(),
            storage,
            fire_on: TriggerType::AfterWatermark,
            fire_early: false,
            fire_late: false,
            re_fire: false,
        }
    }

    pub(crate) fn select(&mut self, windows: HashMap<WindowDescriptor, bool>, current: &Time) -> Vec<(WindowDescriptor, Train)> {
        let mut trains = vec![];
        windows.into_iter().for_each(|(window, is_complete)| {
            let mut trigger = false;
            if let Some(status) = self.triggered_windows.get(&window) {
                // have already seen this window
                if self.re_fire && status == &TriggerStatus::Early && &window.to <= current  {
                    // still early
                    trigger = true;
                }else if self.fire_late && status == &TriggerStatus::OnTime {
                    // we fired on time already and re-fire late
                    trigger = true;
                    self.triggered_windows.insert(window, TriggerStatus::Late);
                }
            }else {
                // have not seen this window
                if &window.to <= current{
                    // on time, did not fire yet
                    trigger = true;
                    self.triggered_windows.insert(window, TriggerStatus::OnTime);
                }else if self.fire_early{
                    // early fire
                    trigger = true;
                    self.triggered_windows.insert(window, TriggerStatus::Early);
                }
            }
            if trigger {
                match self.get_trains(window) {
                    None => {}
                    Some(t) => trains.push((window,t)),
                }
            }
        });

        trains
    }

    fn get_trains(&self, window: WindowDescriptor) -> Option<Train> {
        let storage = self.storage.lock().unwrap();
        storage
            .iter()
            .filter(|train| window.from <= train.event_time && window.to >= train.event_time)
            .cloned()
            .reduce(|a,b| {
                a + b
            })
    }
}

pub enum TriggerType{
    AfterWatermark,
    OnElement
}



#[derive(Clone,PartialEq)]
pub enum TriggerStatus {
    Early,
    OnTime,
    Late
}
