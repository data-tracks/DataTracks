use std::thread;

use crate::processing::receiver::Receiver;
use crate::processing::sender::Sender;
use crate::processing::transform::Transform;
use crate::processing::window::Window;

struct Station {
    id: i64,
    receiver: Receiver,
    sender: Sender,
    window: Window,
    transform: Transform,
}

impl Station {
    fn new(id: i64) -> Self {
        let station = Station {
            id,
            sender: Sender::new(),
            receiver: Receiver::new(),
            window: Window::new(),
            transform: Transform::default(),
        };
        station
    }

    fn operate(&self) {
        let handler = thread::spawn(move || {
            while let Ok(train) = self.receiver.recv() {
                let transformed = self.transform.apply(self.window.apply(train));
            }
        });
    }
}