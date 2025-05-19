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
use crate::processing::window::Window;
use crate::processing::Train;
use crate::util::{new_id, Rx};
use crossbeam::channel;
use crossbeam::channel::{unbounded, Receiver};
pub use logos::{Source};
use tracing::debug;

const IDLE_TIMEOUT: Duration = Duration::from_nanos(10);

pub(crate) struct Platform {
    id: usize,
    control: Receiver<Command>,
    receiver: Rx<Train>,
    sender: Option<Sender>,
    transform: Option<Transform>,
    layout: Layout,
    window: Window,
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

        (
            Platform {
                id: new_id(),
                control: control.1,
                receiver,
                sender: Some(sender),
                transform,
                window,
                layout,
                stop,
                blocks,
                inputs,
                transforms,
            },
            control.0,
        )
    }
    pub(crate) fn operate(&mut self, control: Arc<channel::Sender<Command>>) {
        let process = optimize(
            self.stop,
            self.transform.clone(),
            self.window.windowing(),
            self.transforms.clone(),
        );
        let sender = self.sender.take().unwrap();

        let stop = self.stop;
        let timeout = IDLE_TIMEOUT;
        let mut threshold = 2000;
        let mut too_high = false;

        let mut block = Block::new(self.inputs.clone(), self.blocks.clone(), process, sender);

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
                    Command::Attach(num, send) => {
                        block.add(num, send);
                    }
                    Command::Detach(num) => {
                        block.remove(num);
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
    mut window: Box<dyn Taker>,
    transforms: HashMap<String, Transform>,
) -> MutWagonsFunc {
    if let Some(transform) = transform {
        let mut enumerator = transform.optimize(transforms, Some(OptimizeStrategy::rule_based()));
        Box::new(move |train| {
            let windowed = window.take(train);
            enumerator.dynamically_load(windowed);
            return enumerator.drain_to_train(stop);
        })
    } else {
        Box::new(move |trains| {
            let windowed = window.take(trains);
            let train: Train = windowed.into();
            return train.mark(stop);
        })
    }
}
