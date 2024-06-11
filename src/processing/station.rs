use std::sync::mpsc;
use std::sync::mpsc::{channel, Receiver};
use std::thread;
use std::thread::JoinHandle;
use crate::processing::block::Block;

use crate::processing::sender::Sender;
use crate::processing::train::Train;
use crate::processing::transform::Transform;
use crate::processing::window::Window;
use crate::util::GLOBAL_ID;

pub(crate) struct Station {
    id: i64,
    pub stop: i64,
    send: mpsc::Sender<Train>,
    receiver: Option<Receiver<Train>>,
    sender: Option<Sender>,
    window: Window,
    transform: Transform,
    block: Block,
    handlers: Vec<JoinHandle<()>>,
}

impl Station {
    pub(crate) fn default() -> Self {
        Self::new(-1)
    }

    pub(crate) fn new(stop: i64) -> Self {
        let (tx, rx) = channel();
        let station = Station {
            id: GLOBAL_ID.new_id(),
            stop,
            send: tx,
            receiver: Some(rx),
            sender: Some(Sender::new()),
            window: Window::default(),
            transform: Transform::default(),
            block: Block::default(),
            handlers: vec![],
        };
        station
    }
    pub(crate) fn merge(&mut self, other: Station) {
        self.block = other.block;
    }

    pub(crate) fn stop(&mut self, stop: i64) {
        self.stop = stop
    }

    pub(crate) fn window(&mut self, window: Window) {
        self.window = window;
    }

    pub(crate) fn transform(&mut self, transform: Transform) {
        self.transform = transform;
    }

    pub(crate) fn block(&mut self, block: Block) {
        self.block = block;
    }

    pub(crate) fn add_out(&mut self, id: i64, out: mpsc::Sender<Train>) {
        let mut option = self.sender.take();
        if let Some(ref mut sender) = option {
            sender.add(id, out);
        }
        self.sender = option;
    }

    pub(crate) fn send(&self, train: Train) {
        self.send.send(train).unwrap();
    }

    pub fn dump(&self) -> String {
        let mut dump = self.stop.to_string();
        dump += &self.transform.dump();
        dump
    }

    pub(crate) fn operate(&mut self) {
        let receiver = self.receiver.take().unwrap();
        let sender = self.sender.take().unwrap();
        let transform = self.transform.transformer();
        let window = self.window.windowing();

        let handle = thread::spawn(move || {
            while let Ok(train) = receiver.recv() {
                let transformed = transform(window(train));
                sender.send(transformed)
            }
        });
        self.handlers.push(handle)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;

    use crate::processing::station::Station;
    use crate::processing::train::Train;
    use crate::value::Value;

    #[test]
    fn start_stop_test() {
        let mut station = Station::new(0);

        let values = vec![Value::text("test"), Value::bool(true), Value::float(3.3), Value::null()];

        let (tx, rx) = channel();

        station.add_out(0, tx);
        station.operate();
        station.send.send(Train::new(values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(t) => {
                assert_eq!(values.len(), t.values.len());
                for (i, value) in t.values.iter().enumerate() {
                    assert_eq!(*value, values[i]);
                    assert_ne!(Value::text(""), *value)
                }
            }
            Err(..) => assert!(false),
        }
    }

    #[test]
    fn stencil_transform() {
        let stencils = vec![
            "1-2{sql|SELECT * FROM $1}",
        ];

        for stencil in stencils {
            let plan = crate::processing::plan::Plan::parse(stencil);
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn stencil_window() {
        let stencils = vec![
            "1-2(3s)",
        ];

        for stencil in stencils {
            let plan = crate::processing::plan::Plan::parse(stencil);
            assert_eq!(plan.dump(), stencil)
        }
    }
}