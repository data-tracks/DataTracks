use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use crate::processing::receiver::Receiver;
use crate::processing::sender::Sender;
use crate::processing::transform::Transform;
use crate::processing::window::Window;

struct Station {
    id: i64,
    receiver: Option<Receiver>,
    sender: Arc<Sender>,
    window: Arc<Window>,
    transform: Arc<Transform>,
    handlers: Vec<JoinHandle<()>>,
}

impl Station {
    fn new(id: i64) -> Self {
        let station = Station {
            id,
            sender: Arc::new(Sender::new()),
            receiver: Some(Receiver::new()),
            window: Arc::new(Window::default()),
            transform: Arc::new(Transform::default()),
            handlers: vec![]
        };
        station
    }

    fn operate(&mut self) {
        let receiver = self.receiver.take().unwrap();
        let sender = self.sender.clone();
        let transform = self.transform.clone();
        let window = self.window.clone();

        let handle = thread::spawn(move || {
            while let Ok(train) = receiver.recv() {
                let transformed = transform.apply(window.apply(train));
                sender.send(transformed)
            }
        });
        self.handlers.push(handle)
    }
}

#[cfg(test)]
mod tests{
    use crate::processing::station::Station;
    use crate::processing::train::Train;
    use crate::value::Value;

    #[test]
    fn start_stop_test(){
        let mut station = Station::new(0);

        let values = vec![Value::string("test")];

        station.operate();
        station.sender.send(Train::new(values.clone()));

        let res = station.sender.;
        match res {
            Ok(t) => {
                for (i, value) in t.values.iter().enumerate() {
                    assert_eq!(*value, values[i])
                }
            },
            Err(..) => assert!(false),
        }
    }
}