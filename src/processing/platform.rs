use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam::channel;
use crossbeam::channel::{Receiver, unbounded};

use crate::algebra::RefHandler;
use crate::processing::block::Block;
use crate::processing::sender::Sender;
use crate::processing::station::{Command, Station};
use crate::processing::station::Command::READY;
use crate::processing::Train;
use crate::processing::transform::Taker;

pub(crate) struct Platform {
    control: Receiver<Command>,
    receiver: Receiver<Train>,
    sender: Option<Sender>,
    transform: Option<Box<dyn RefHandler>>,
    window: Option<Taker>,
    stop: i64,
    blocks: Vec<i64>,
    inputs: Vec<i64>,
}

impl Platform {
    pub(crate) fn new(station: &mut Station) -> (Self, channel::Sender<Command>) {
        let receiver = station.incoming.1.clone();
        let sender = station.outgoing.clone();
        let transform = station.transform.transformer();
        let window = station.window.windowing();
        let stop = station.stop;
        let blocks = station.block.clone();
        let inputs = station.inputs.clone();
        let control = unbounded();

        (Platform { control: control.1, receiver, sender: Some(sender), transform: Some(transform), window: Some(window), stop, blocks, inputs }, control.0)
    }
    pub(crate) fn operate(&mut self, control: Arc<channel::Sender<Command>>) {
        let transform = self.transform.take().unwrap();
        let stop = self.stop.clone();
        let window = self.window.take().unwrap();
        let sender = self.sender.take().unwrap();
        let timeout = Duration::from_nanos(10);

        let process = Box::new(move |trains: &mut Vec<Train>| {
            let mut transformed = transform.process(stop, (window)(trains));
            transformed.last = stop;
            sender.send(transformed)
        });

        let mut block = Block::new(self.inputs.clone(), self.blocks.clone(), process);

        control.send(READY(stop)).unwrap();
        loop {
            // did we get a command?
            match self.control.try_recv() {
                Ok(command) => {
                    match command {
                        Command::STOP(_) => return,
                        _ => {}
                    }
                }
                _ => {}
            };
            match self.receiver.try_recv() {
                Ok(train) => {
                    block.next(train) // window takes precedence to
                }
                _ => {
                    thread::sleep(timeout) // wait again
                }
            };
        }
    }
}
