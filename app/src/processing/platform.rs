use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::{sleep, spawn};
use std::time::Duration;

use crate::new_channel;
use crate::optimize::OptimizeStrategy;
use crate::processing::block::Block;
use crate::processing::select::{TriggerSelector, WindowSelector};
use crate::processing::layout::Layout;
use crate::processing::sender::Sender;
use crate::processing::station::Command::{Okay, Ready, Threshold};
use crate::processing::station::{Command, Station};
use crate::processing::transform::Transform;
use crate::processing::watermark::WatermarkStrategy;
use crate::processing::window::Window;
use crate::processing::Train;
use crate::util::Tx;
use crate::util::{new_id, Rx};
use crossbeam::channel::Receiver;
use crossbeam::{channel, select};
pub use logos::Source;
use tracing::{debug, error};
use value::{Time, Value};

const IDLE_TIMEOUT: Duration = Duration::from_nanos(10);

// What: Transformations, Where: Windowing, When: Triggers, How: Accumulation

pub(crate) struct Platform {
    id: usize,
    control: Receiver<Command>,
    receiver: Rx<Train>,
    sender: Sender,
    transform: Option<Transform>,
    layout: Layout,
    window: Window,
    watermark_strategy: WatermarkStrategy,
    stop: usize,
    blocks: Vec<usize>,
    inputs: Vec<usize>,
    transforms: HashMap<String, Transform>,
}

impl Platform {
    pub(crate) fn new(
        station: &mut Station,
        transforms: HashMap<String, Transform>,
    ) -> (Self, channel::Sender<Command>) {
        let receiver = station.incoming.1.clone();
        let sender = station.outgoing.clone();
        let transform = station.transform.clone();
        let window = station.window.clone();
        let stop = station.stop;
        let blocks = station.block.clone();
        let inputs = station.inputs.clone();
        let control = station.control.clone();
        let layout = station.layout.clone();
        let watermark_strategy = station.watermark_strategy.clone();

        (
            Platform {
                id: new_id(),
                control: control.1,
                receiver,
                sender,
                transform,
                window,
                layout,
                stop,
                blocks,
                inputs,
                transforms,
                watermark_strategy,
            },
            control.0,
        )
    }
    pub(crate) fn operate(&mut self, control: Arc<channel::Sender<Command>>) {
        let process = optimize(
            self.stop,
            self.transform.clone(),
            self.transforms.clone(),
            Box::new(SenderStep {
                sender: self.sender.clone(),
            }),
        );

        let stop = self.stop;
        let timeout = IDLE_TIMEOUT;
        let mut threshold = 2000;
        let mut too_high = false;

        let mut watermark_strategy = Arc::new(self.watermark_strategy.clone());

        let storage = Arc::new(Mutex::new(vec![]));

        let window_selector = WindowSelector::new(storage.clone().into(), self.window.clone());
        let trigger_selector = TriggerSelector::new(storage.clone().into());

        let when_tx = when(watermark_strategy.clone(), window_selector, trigger_selector, process);

        control.send(Ready(stop)).unwrap();

        loop {
            // are we struggling to handle incoming?
            let current = self.receiver.len();
            if current > threshold && !too_high {
                control.send(Threshold(stop)).unwrap();
                too_high = true;
            } else if current < threshold && too_high {
                control.send(Okay(stop)).unwrap();
                too_high = false;
            }

            // did we get a command?
            if let Ok(command) = self.control.try_recv() {
                match command {
                    Command::Stop(_) => {
                        match when_tx.send(Command::Stop(stop)) {
                            Ok(_) => {}
                            Err(err) => {
                                error!("cannot stop trigger {err}")
                            }
                        }
                        return;
                    }
                    Command::Attach(num, (send, watermark)) => {
                        watermark_strategy.attach(num, watermark);
                    }
                    Command::Detach(num) => {
                        watermark_strategy.detach(num);
                    }
                    Threshold(th) => {
                        threshold = th;
                    }
                    _ => {}
                }
            }

            match self.receiver.try_recv() {
                Ok(train) => {
                    debug!("{:?}", train);
                    if self.layout.fits_train(&train) {
                        storage.lock().unwrap().push(train.clone());
                        watermark_strategy.mark(&train);
                    }
                }
                _ => {
                    sleep(timeout); // wait again
                }
            };
        }
    }
}

fn when(
    watermark_strategy: Arc<WatermarkStrategy>,
    mut select: WindowSelector,
    mut trigger: TriggerSelector,
    mut what: Box<dyn Step>,
) -> Tx<Command> {
    let (tx, rx) = new_channel::<Command, &str>("Trigger");
    // shall we?
    // - specific window?
    // - specific watermark
    // - always
    // take what we need -> window
    // - which window?
    // apply transformation
    // send out
    spawn(move || loop {
        if let Ok(command) = rx.recv() {
            match command {
                Command::Stop(_) => return,
                _ => {}
            }
        }
        let current = watermark_strategy.current();

        let windows = select.select(current);

        match trigger.select(windows) {
            trains if trains.len() > 0  => {
                trains.into_iter().for_each(|train| what.apply(train));
            }
            _ => sleep(IDLE_TIMEOUT),
        }
    });

    tx
}

fn optimize(
    stop: usize,
    transform: Option<Transform>,
    transforms: HashMap<String, Transform>,
    mut next: Box<dyn Step>,
) -> Box<dyn Step> {
    match transform {
        Some(transform) => {
            let mut enumerator =
                transform.optimize(transforms, Some(OptimizeStrategy::rule_based()));
            Box::new(FunctionStep {
                function: Box::new(move |train| {
                    enumerator.dynamically_load(train);
                    next.apply(enumerator.drain_to_train(stop));
                }),
            })
        }
        None => next,
    }
}

fn merge_marks(train: &mut Vec<Train>) -> HashMap<usize, Time> {
    // merge watermarks for now
    let mut marks = HashMap::new();
    train.iter().for_each(|t| {
        t.marks.iter().for_each(|(k, v)| {
            marks.insert(*k, v.clone());
        })
    });
    marks
}

pub trait Step {
    fn apply(&mut self, train: Train);

    fn detach(&mut self, num: usize);

    fn attach(&mut self, num: usize, tx: Tx<Train>);
}

pub struct FunctionStep {
    function: Box<dyn FnMut(Train)>,
}

impl FunctionStep {
    pub fn new(function: Box<dyn FnMut(Train)>) -> FunctionStep {
        FunctionStep { function }
    }
}

impl Step for FunctionStep {
    fn apply(&mut self, train: Train) {
        (self.function)(train);
    }

    fn detach(&mut self, _num: usize) {
        // nothing on purpose
    }

    fn attach(&mut self, _num: usize, _tx: Tx<Train>) {
        // nothing on purpose
    }
}

pub struct SenderStep {
    sender: Sender,
}

impl Step for SenderStep {
    fn apply(&mut self, train: Train) {
        self.sender.send(train)
    }

    fn detach(&mut self, num: usize) {
        self.sender.remove(num)
    }

    fn attach(&mut self, num: usize, tx: Tx<Train>) {
        self.sender.add(num, tx)
    }
}


pub struct WindowOperator {
    storage: Arc<Mutex<Vec<Train>>>,
    window: Window,
}

impl WindowOperator {
    pub fn new(window: Window, storage: Arc<Mutex<Vec<Train>>>) -> Self {
        WindowOperator { storage, window }
    }

    pub fn get(&self, time: &Time) -> Vec<Train> {
        match &self.window {
            Window::Non(_) => self.storage.lock().unwrap().clone(),
            Window::Back(b) => {
                let back = time - b.duration;
                self.storage
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|t| &t.event_time <= time && t.event_time >= back)
                    .collect()
            }
            Window::Interval(i) => {
                let (from, to) = i.get_times(time);
                self.storage
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|t| &t.event_time <= &to && t.event_time >= from)
                    .collect()
            }
        }
    }
}
