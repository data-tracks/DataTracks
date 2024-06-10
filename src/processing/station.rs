use std::sync::mpsc;
use std::sync::mpsc::{channel, Receiver};
use std::thread;
use std::thread::JoinHandle;

use crate::processing::sender::Sender;
use crate::processing::train::Train;
use crate::processing::transform::Transform;
use crate::processing::window::Window;
use crate::util::GLOBAL_ID;

pub(crate) struct Station {
    id: i64,
    stop: i64,
    send: mpsc::Sender<Train>,
    receiver: Option<Receiver<Train>>,
    sender: Option<Sender>,
    window: Window,
    transform: Transform,
    handlers: Vec<JoinHandle<()>>,
}

impl Station {
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
            handlers: vec![],
        };
        station
    }

    pub(crate) fn transform(&mut self, transform: Transform) {
        self.transform = transform;
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

    pub(crate) fn operate(&mut self) {
        let receiver = self.receiver.take().unwrap();
        let sender = self.sender.take().unwrap();
        let transform = self.transform.func.take().unwrap();
        let window = self.window.func.take().unwrap();

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

    fn transform_test() {}
}