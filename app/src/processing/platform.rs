use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::optimize::OptimizeStrategy;
use crate::processing::block::Block;
use crate::processing::layout::Layout;
use crate::processing::sender::Sender;
use crate::processing::station::Command::{Okay, Ready, Threshold};
use crate::processing::station::{Command, Station};
use crate::processing::train::MutWagonsFunc;
use crate::processing::transform::{Taker, Transform};
use crate::processing::watermark::WatermarkStrategy;
use crate::processing::window::Window;
use crate::processing::Block::{All, Non, Specific};
use crate::processing::{watermark, Train};
use crate::util::{new_id, Rx};
use crossbeam::channel;
use crossbeam::channel::Receiver;
pub use logos::Source;
use tracing::debug;
use value::Time;
use crate::Tx;

const IDLE_TIMEOUT: Duration = Duration::from_nanos(10);

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

        let watermark_strategy = self.watermark_strategy.clone();
        let mut block = Block::new(self.inputs.clone(), self.blocks.clone(), process);
        

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
                    Command::Stop(_) => return,
                    Command::Attach(num, (send, watermark)) => {
                        block.attach(num, send);
                        self.watermark_strategy.attach(num, watermark);
                    }
                    Command::Detach(num) => {
                        block.detach(num);
                        self.watermark_strategy.detach(num);
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
                        self.watermark_strategy.mark(&train);
                        block.next(train); // window takes precedence to
                    }
                }
                _ => {
                    thread::sleep(timeout); // wait again
                }
            };
        }
    }
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
