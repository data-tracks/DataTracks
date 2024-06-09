use std::sync::mpsc;
use std::sync::mpsc::channel;

use crate::processing::train::Train;

pub(crate) struct Receiver {
    sender: mpsc::Sender<Train>,
    receiver: mpsc::Receiver<Train>,
}

impl Receiver {
    pub(crate) fn new() -> Self {
        let (tx, rx) = channel();
        Receiver {
            sender: tx,
            receiver: rx,
        }
    }
}

