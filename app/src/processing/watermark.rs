use std::sync::Mutex;
use chrono::Duration;
use value::Time;
use crate::processing::Train;
use crate::util::Tx;

#[derive(Clone)]
pub enum WatermarkStrategy {
    Monotonic(MonotonicWatermark), // every point
    Periodic(PeriodicWatermark), // offset delay every point
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
                m.mark(train);
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
            WatermarkStrategy::Monotonic(m) => {
                m.current()
            }
            WatermarkStrategy::Periodic(p) => {
                p.current()
            }
            WatermarkStrategy::Punctuated(p) => {
                p.current()
            }
        }
    }
}

#[derive(Default)]
pub struct MonotonicWatermark {
    last: Mutex<Time>,
}


impl MonotonicWatermark {
    pub fn new() -> Self {
        MonotonicWatermark::default()
    }

    pub(crate) fn mark(&self, train: &Train) {
        let mut last = self.last.lock().unwrap();
        if train.event_time > *last {
            *last = train.event_time.clone();
        }
    }

    pub(crate) fn detach(&self, num: usize) {
        todo!()
    }

    pub(crate) fn attach(&self, num: usize, sender: Tx<Time>) {
        todo!()
    }

    pub(crate) fn current(&self) -> Time {
        self.last.lock().unwrap().clone()
    }
}

impl Clone for MonotonicWatermark {
    fn clone(&self) -> Self {
        MonotonicWatermark::new()
    }
}

pub struct PeriodicWatermark {
    mark: Mutex<Time>,
    offset: Offset,
}


impl PeriodicWatermark {
    pub fn new(offset: Offset) -> Self {
        PeriodicWatermark{ mark: Default::default(), offset }
    }

    pub(crate) fn mark(&self, train: &Train) {
        let time = self.offset.apply(&train.event_time);
        let mut mark = self.mark.lock().unwrap();
        if time > *mark {
            *mark = time;
        }
    }

    pub(crate) fn current(&self) -> Time {
        self.mark.lock().unwrap().clone()
    }

    pub(crate) fn detach(&self, num: usize) {
        todo!()
    }
    pub(crate) fn attach(&self, num: usize, sender: Tx<Time>) {
        todo!()
    }
}

impl Clone for PeriodicWatermark {
    fn clone(&self) -> Self {
        PeriodicWatermark::new(self.offset.clone())
    }
}

#[derive(Clone)]
pub struct Offset{
    duration: Duration
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


impl PunctuatedWatermark {
    pub fn new() -> Self {
        PunctuatedWatermark{ mark: Default::default() }
    }

    pub(crate) fn detach(&self, num: usize) {
        todo!()
    }

    pub(crate) fn attach(&self, num: usize, sender: Tx<Time>) {
        todo!()
    }

    pub(crate) fn mark(&self, train: &Train) {
        todo!()
    }

    pub(crate) fn current(&self) -> Time {
        self.mark
    }
}


