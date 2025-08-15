use crate::channel::Tx;
use std::fmt::{Debug, Display, Formatter};
use value::train::Train;
use value::Time;

#[derive(Clone)]
pub enum Command {
    Stop(usize),
    Ready(usize),
    Overflow(usize),
    Threshold(usize),
    Okay(usize),
    Attach(usize, (Tx<Train>, Tx<Time>)),
    Detach(usize),
}

impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Command::Ready(a), Command::Ready(b)) => a.eq(b),
            (Command::Stop(a), Command::Stop(b)) => a.eq(b),
            (Command::Overflow(a), Command::Overflow(b)) => a.eq(b),
            (Command::Threshold(a), Command::Threshold(b)) => a.eq(b),
            (Command::Okay(a), Command::Okay(b)) => a.eq(b),
            (Command::Attach(a, _), Command::Attach(b, _)) => a.eq(b),
            (Command::Detach(a), Command::Detach(b)) => a.eq(b),
            _ => false,
        }
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Stop(s) => f.write_str(format!("Stop: {}", s).as_str()),
            Command::Ready(r) => f.write_str(format!("Ready: {}", r).as_str()),
            Command::Overflow(o) => f.write_str(format!("Overflow: {}", o).as_str()),
            Command::Threshold(t) => f.write_str(format!("Threshold: {}", t).as_str()),
            Command::Okay(o) => f.write_str(format!("Okay: {}", o).as_str()),
            Command::Attach(a, _) => f.write_str(format!("Attach: {}", a).as_str()),
            Command::Detach(d) => f.write_str(format!("Detach: {}", d).as_str()),
        }
    }
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Stop(s) => f.debug_tuple("Stop").field(s).finish(),
            Command::Ready(r) => f.debug_tuple("Ready").field(r).finish(),
            Command::Overflow(o) => f.debug_tuple("Overflow").field(o).finish(),
            Command::Threshold(t) => f.debug_tuple("Threshold").field(t).finish(),
            Command::Okay(o) => f.debug_tuple("Okay").field(o).finish(),
            Command::Attach(id, _) => f.debug_tuple("Attach").field(id).finish(),
            Command::Detach(id) => f.debug_tuple("Detach").field(id).finish(),
        }
    }
}
