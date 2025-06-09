use crate::util::broadcast::{BroadcastRx, BroadcastTx};
use crate::util::one_shot;

use crate::util::one_shot::{SingleRx, SingleTx};

#[derive(Clone)]
pub enum Tx<T: Clone + Send + 'static> {
    Single(SingleTx<T>),
    Broadcast(BroadcastTx<T>),
}

impl<T: Clone + Send + 'static> Tx<T> {
    pub(crate) fn name(&self) -> String {
        match self {
            Tx::Single(s) => s.name(),
            Tx::Broadcast(b) => b.name(),
        }
    }

    pub(crate) fn subscribe(&self) -> Rx<T> {
        match self {
            Tx::Single(s) => Rx::Single(s.subscribe()),
            Tx::Broadcast(b) => Rx::Broadcast(b.subscribe()),
        }
    }

    pub fn send(&self, item: T) {
        match self {
            Tx::Single(s) => s.send(item).unwrap(),
            Tx::Broadcast(s) => s.send(item),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Tx::Single(s) => s.len(),
            Tx::Broadcast(b) => b.len(),
        }
    }
}

#[derive(Clone)]
pub enum Rx<T: Clone + Send + 'static> {
    Single(SingleRx<T>),
    Broadcast(BroadcastRx<T>),
}

impl<T: Clone + Send + 'static> Rx<T> {
    pub(crate) fn try_recv(&self) -> Result<T, String> {
        match self {
            Rx::Single(s) => s.try_recv(),
            Rx::Broadcast(b) => b.try_recv(),
        }
    }

    pub fn recv(&self) -> Result<T, String> {
        match self {
            Rx::Single(s) => s.recv(),
            Rx::Broadcast(b) => b.recv(),
        }
    }
}

impl<T: Clone + Send + 'static> Rx<T> {
    pub fn len(&self) -> usize {
        match self {
            Rx::Single(i) => i.len(),
            Rx::Broadcast(r) => r.len(),
        }
    }
}

pub fn new_broadcast<Msg: Clone + Send + 'static, S: AsRef<str>>(name: S) -> Tx<Msg> {
    Tx::Broadcast(BroadcastTx::new_empty_channel(name))
}

pub fn new_channel<Msg: Clone + Send + 'static, S: AsRef<str>>(
    name: S,
    broadcast: bool,
) -> (Tx<Msg>, Rx<Msg>) {
    if broadcast {
        let (tx, rx) = BroadcastTx::new_channel(name);
        (Tx::Broadcast(tx), Rx::Broadcast(rx))
    } else {
        let (tx, rx) = one_shot::new_channel(name);
        (Tx::Single(tx), Rx::Single(rx))
    }
}
