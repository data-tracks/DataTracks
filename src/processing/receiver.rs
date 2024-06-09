use std::sync::mpsc;
use std::sync::mpsc::{channel, RecvError};

use crate::processing::train::Train;

pub(crate) struct Receiver {
    pub tx: mpsc::Sender<Train>,
    pub rx: mpsc::Receiver<Train>,
}

impl Receiver {
    pub(crate) fn new() -> Self {
        let (tx, rx) = channel();
        Receiver {
            tx,
            rx,
        }
    }
    pub(crate) fn recv(&self) -> Result<Train, RecvError> {
        self.rx.recv()
    }
}

