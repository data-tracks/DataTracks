use crate::processing::destination::Destination;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::util::{new_channel, Rx, Tx, GLOBAL_ID};
use crossbeam::channel::{unbounded, Sender};
use std::sync::Arc;
use std::thread;
use tracing::{debug, error};

pub struct DebugDestination {
    id: i64,
    stop: i64,
    receiver: Option<Rx<Train>>,
    sender: Tx<Train>,
}

impl DebugDestination {
    pub fn new(stop: i64) -> Self {
        let (tx, num, rx) = new_channel();
        DebugDestination { id: GLOBAL_ID.new_id(), stop, receiver: Some(rx), sender: tx }
    }
}

impl Destination for DebugDestination {
    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        let receiver = self.receiver.take().unwrap();
        let (tx, rx) = unbounded();

        thread::spawn(move || {
            loop {
                let res = receiver.recv();
                match res {
                    Ok(train) => {
                        debug!("{:?}", train);
                    }
                    Err(e) => {
                        error!("{}", e)
                    }
                }
            }
        });
        tx
    }

    fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }

    fn get_stop(&self) -> i64 {
        self.stop
    }

    fn get_id(&self) -> i64 {
        self.id
    }
}