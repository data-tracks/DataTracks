use crate::processing::Train;
use crate::util::Tx;
use chrono::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use error::error::TrackError;
use value::Time;

/// Thread-safe and shareable watermarks
#[derive(Clone)]
pub enum WatermarkStrategy {
    Monotonic(MonotonicWatermark),   // every point
    Periodic(PeriodicWatermark),     // offset delay every point
    Punctuated(PunctuatedWatermark), // on certain event
}

impl Default for WatermarkStrategy {
    fn default() -> Self {
        WatermarkStrategy::Monotonic(MonotonicWatermark::default())
    }
}

impl WatermarkStrategy {
    pub(crate) fn mark(&self, train: &Train) {
        match self {
            WatermarkStrategy::Monotonic(m) => {
                m.mark(train).unwrap();
            }
            WatermarkStrategy::Periodic(p) => {
                p.mark(train);
            }
            WatermarkStrategy::Punctuated(p) => {
                p.mark(train);
            }
        }
    }

    pub fn attach(&self, num: usize, sender: Tx<Time>) {
        match self {
            WatermarkStrategy::Monotonic(m) => m.attach(num, sender),
            WatermarkStrategy::Periodic(p) => p.attach(num, sender),
            WatermarkStrategy::Punctuated(p) => p.attach(num, sender),
        }
    }

    pub(crate) fn detach(&self, num: usize) {
        match self {
            WatermarkStrategy::Monotonic(m) => m.detach(num),
            WatermarkStrategy::Periodic(p) => p.detach(num),
            WatermarkStrategy::Punctuated(p) => p.detach(num),
        }
    }

    pub(crate) fn current(&self) -> Time {
        match self {
            WatermarkStrategy::Monotonic(m) => m.current(),
            WatermarkStrategy::Periodic(p) => p.current(),
            WatermarkStrategy::Punctuated(p) => p.current(),
        }
    }
}

pub struct MonotonicWatermark {
    last: Arc<Mutex<Time>>,
    observers: Arc<Mutex<HashMap<usize, Tx<Time>>>>,
}

impl Default for MonotonicWatermark {
    fn default() -> Self {
        #[cfg(test)]
        {
            return MonotonicWatermark {
                last: Arc::new(Mutex::new(Time::new(0, 0))),
                observers: Arc::new(Mutex::new(HashMap::new())),
            };
        }
        #[cfg(not(test))]
        {
            MonotonicWatermark {
                last: Arc::new(Mutex::new(Time::default())),
                observers: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }
}

impl Clone for MonotonicWatermark {
    fn clone(&self) -> Self {
        MonotonicWatermark {
            last: self.last.clone(),
            observers: self.observers.clone(),
        }
    }
}

impl MonotonicWatermark {
    pub fn new() -> Self {
        MonotonicWatermark::default()
    }

    pub(crate) fn mark(&self, train: &Train) -> Result<(), TrackError> {
        let mut last = self.last.lock().unwrap();
        let time = train.event_time;
        if time > *last {
            *last = train.event_time;
            drop(last);

            self.observers
                .lock()
                .unwrap()
                .values()
                .try_for_each(|observer| observer.send(time))?;
        }
        Ok(())
    }

    pub(crate) fn detach(&self, num: usize) {
        self.observers.lock().unwrap().remove(&num);
    }

    pub(crate) fn attach(&self, num: usize, sender: Tx<Time>) {
        self.observers.lock().unwrap().insert(num, sender);
    }

    pub(crate) fn current(&self) -> Time {
        *self.last.lock().unwrap()
    }
}

pub struct PeriodicWatermark {
    mark: Mutex<Time>,
    offset: Offset,
}

impl PeriodicWatermark {
    pub fn new(offset: Offset) -> Self {
        PeriodicWatermark {
            mark: Default::default(),
            offset,
        }
    }

    pub(crate) fn mark(&self, train: &Train) {
        let time = self.offset.apply(&train.event_time);
        let mut mark = self.mark.lock().unwrap();
        if time > *mark {
            *mark = time;
        }
    }

    pub(crate) fn current(&self) -> Time {
        *self.mark.lock().unwrap()
    }

    pub(crate) fn detach(&self, _num: usize) {
        todo!()
    }
    pub(crate) fn attach(&self, _num: usize, _sender: Tx<Time>) {
        todo!()
    }
}

impl Clone for PeriodicWatermark {
    fn clone(&self) -> Self {
        PeriodicWatermark::new(self.offset.clone())
    }
}

#[derive(Clone)]
pub struct Offset {
    duration: Duration,
}

impl Offset {
    fn apply(&self, time: &Time) -> Time {
        time - self.duration
    }
}

#[derive(Clone)]
pub struct PunctuatedWatermark {
    mark: Time,
}

impl Default for PunctuatedWatermark {
    fn default() -> Self {
        Self::new()
    }
}

impl PunctuatedWatermark {
    pub fn new() -> Self {
        PunctuatedWatermark {
            mark: Default::default(),
        }
    }

    pub(crate) fn detach(&self, _num: usize) {
        todo!()
    }

    pub(crate) fn attach(&self, _num: usize, _sender: Tx<Time>) {
        todo!()
    }

    pub(crate) fn mark(&self, _train: &Train) {
        todo!()
    }

    pub(crate) fn current(&self) -> Time {
        self.mark
    }
}
