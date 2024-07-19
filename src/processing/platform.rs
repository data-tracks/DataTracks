use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use crossbeam::channel;
use crossbeam::channel::{Receiver, unbounded};

use crate::processing::block::Block;
use crate::processing::sender::Sender;
use crate::processing::station::{Command, Station};
use crate::processing::station::Command::{Okay, Ready, Threshold};
use crate::processing::Train;
use crate::processing::train::MutWagonsFunc;
use crate::processing::transform::{Taker, Transform};
use crate::processing::window::Window;
use crate::util::{GLOBAL_ID, Rx};

pub(crate) struct Platform {
    id: i64,
    control: Receiver<Command>,
    receiver: Rx<Train>,
    sender: Option<Sender>,
    transform: HashMap<i64, Transform>,
    window: Window,
    stop: i64,
    blocks: Vec<i64>,
    inputs: Vec<i64>,
    incoming: Arc<AtomicU64>
}

impl Platform {
    pub(crate) fn new(station: &mut Station) -> (Self, channel::Sender<Command>) {
        let receiver = station.incoming.2.clone();
        let counter = station.incoming.1.clone();
        let sender = station.outgoing.clone();
        let transform = station.transform.clone();
        let window = station.window.clone();
        let stop = station.stop;
        let blocks = station.block.clone();
        let inputs = station.inputs.clone();
        let control = unbounded();

        (Platform {
            id: GLOBAL_ID.new_id(),
            control: control.1,
            receiver,
            sender: Some(sender),
            transform,
            window,
            stop,
            blocks,
            inputs,
            incoming: counter,
        }, control.0)
    }
    pub(crate) fn operate(&mut self, control: Arc<channel::Sender<Command>>) {
        let process = optimize(self.stop, self.transform.clone(), self.window.windowing(), self.sender.take().unwrap());
        let stop = self.stop;
        let timeout = Duration::from_nanos(10);
        let mut threshold = 2000;
        let mut too_high = false;


        let mut block = Block::new(self.inputs.clone(), self.blocks.clone(), process);

        control.send(Ready(stop)).unwrap();

        loop {
            // are we struggling to handle incoming
            let current = self.incoming.load(Ordering::SeqCst);
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
                        Threshold(th) => {
                            threshold = th as u64;
                        }
                        _ => {}
                    }
                }

            match self.receiver.try_recv() {
                Ok(train) => {
                    block.next(train); // window takes precedence to
                }
                _ => {
                    thread::sleep(timeout); // wait again
                }
            };
        }
    }
}

fn optimize(stop: i64, transforms: HashMap<i64, Transform>, mut window: Box<dyn Taker>, sender: Sender) -> MutWagonsFunc {
    return if transforms.is_empty() {
        Box::new(move |trains| {
            let windowed = window.take(trains);
            let mut train:Train = windowed.into();
            train.last = stop;
            sender.send(train);
        })
    }else if transforms.len() == 1 {
        Box::new(move |trains|{
            let windowed = window.take(trains);
            let mut train = transforms.values().last().unwrap().apply(stop, windowed);
            train.last = stop;
            sender.send(train);
        })
    }else{
        Box::new(move |trains| {
            let windowed = window.take(trains);
            let mut trains = HashMap::new();
            for train in windowed {
                trains.entry(train.last).or_insert_with(Vec::new).push(train);
            }
            for (num, trains) in trains {
                let mut train = transforms.get(&num).unwrap().apply(stop, trains);
                train.last = stop;
                sender.send_to(num, train);
            }
        })
    }
}
