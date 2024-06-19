use std::sync::mpsc;
use std::sync::mpsc::{channel, Receiver};
use std::thread;
use std::thread::JoinHandle;

use crate::processing::plan::PlanStage;
use crate::processing::sender::Sender;
use crate::processing::train::Train;
use crate::processing::transform::Transform;
use crate::processing::window::Window;
use crate::util::GLOBAL_ID;

pub(crate) struct Station {
    id: i64,
    pub stop: i64,
    sender_in: Option<mpsc::Sender<Train>>, // to hang up
    receiver: Option<Receiver<Train>>,
    sender: Option<Sender>,
    window: Window,
    transform: Transform,
    block: Vec<i64>,
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
            sender_in: Some(tx),
            receiver: Some(rx),
            sender: Some(Sender::new()),
            window: Window::default(),
            transform: Transform::default(),
            block: vec![],
        };
        station
    }

    pub(crate) fn parse(last: Option<i64>, parts: Vec<(PlanStage, String)>) -> Self {
        let mut station: Station = Station::default();
        for stage in parts {
            match stage.0 {
                PlanStage::WindowStage => station.set_window(Window::parse(stage.1)),
                PlanStage::TransformStage => station.set_transform(Transform::parse(stage.1).unwrap()),
                PlanStage::BlockStage => station.add_block(last.unwrap_or(-1)),
                PlanStage::Num => station.set_stop(stage.1.parse::<i64>().unwrap()),
            }
        }
        station
    }

    pub(crate) fn merge(&mut self, other: Station) {
        for line in other.block {
            self.block.push(line)
        }
    }

    pub(crate) fn close(&mut self) {
        drop(self.sender_in.take())
    }

    pub(crate) fn set_stop(&mut self, stop: i64) {
        self.stop = stop
    }

    pub(crate) fn set_window(&mut self, window: Window) {
        self.window = window;
    }

    pub(crate) fn set_transform(&mut self, transform: Transform) {
        self.transform = transform;
    }

    pub(crate) fn add_block(&mut self, line: i64) {
        self.block.push(line);
    }

    pub(crate) fn add_out(&mut self, id: i64, out: mpsc::Sender<Train>) -> Result<(), String> {
        self.sender.as_mut().ok_or("Could not register sender.".to_string())?.add(id, out);
        Ok(())
    }

    pub(crate) fn send(&mut self, train: Train) -> Result<(), String> {
        self.sender_in.as_ref().ok_or("sender already disconnected.".to_string())?.send(train).map_err(|e| e.to_string())
    }

    pub fn dump(&self, line: i64) -> String {
        let mut dump = "".to_string();
        if self.block.contains(&line) {
            dump += "|";
        }
        dump += &self.stop.to_string();
        dump += &self.window.dump();
        dump += &self.transform.dump();
        dump
    }

    pub(crate) fn get_in(&mut self) -> mpsc::Sender<Train> {
        let sender = self.sender_in.take().unwrap();
        let cloned_sender = sender.clone();
        self.sender_in = Some(sender);
        cloned_sender
    }

    pub(crate) fn operate(&mut self) -> JoinHandle<()> {
        let receiver = self.receiver.take().unwrap();
        let sender = self.sender.take().unwrap();
        let transform = self.transform.transformer();
        let window = self.window.windowing();
        let stop = self.stop;

        thread::spawn(move || {
            let block = Block::new();
            while let Ok(train) = receiver.recv() {
                let temp = window(train);
                let transformed = transform.process(stop, &mut vec![temp]);
                sender.send(transformed)
            }
        })
    }
}


#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;

    use crate::processing::plan::Plan;
    use crate::processing::station::Station;
    use crate::processing::train::Train;
    use crate::value::Value;

    #[test]
    fn start_stop_test() {
        let mut station = Station::new(0);

        let mut values = vec![Value::text("test"), Value::bool(true), Value::float(3.3), Value::null()];

        for x in 0..1_000_000 {
            values.push(Value::int(x))
        }


        let (tx, rx) = channel();

        station.add_out(0, tx).unwrap();
        station.operate();
        station.sender_in.take().unwrap().send(Train::new(0, values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(mut t) => {
                assert_eq!(values.len(), t.values.clone().map_or(usize::MAX, |values| values.len()));
                for (i, value) in t.values.take().unwrap().iter().enumerate() {
                    assert_eq!(value, &values[i]);
                    assert_ne!(&Value::text(""), value)
                }
            }
            Err(..) => assert!(false),
        }
    }


    #[test]
    fn station_two_train() {
        let values = vec![3.into()];

        let mut first = Station::new(0);
        let input = first.get_in();

        let (output_tx, output_rx) = channel();

        let mut second = Station::new(1);
        second.add_out(0, output_tx).unwrap();

        let tx = second.get_in();
        first.add_out(1, tx).unwrap();

        first.operate();
        second.operate();

        input.send(Train::new(0, values.clone())).unwrap();

        let res = output_rx.recv().unwrap();
        assert_eq!(res.values.clone().unwrap(), values);
        assert_ne!(res.values.clone().unwrap(), vec![Value::null()]);

        assert!(output_rx.try_recv().is_err());


        drop(input); // close the channel
        first.close();
        second.close();
    }

    #[test]
    fn sql_parse_block() {
        let stencil = "1-|3{sql|Select * From $1}";

        let mut plan = Plan::parse(stencil);

        let station = plan.stations.get(&3).unwrap();

        assert!(station.block.contains(&1));
    }
}