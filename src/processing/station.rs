use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use crossbeam::channel;
use crossbeam::channel::{Receiver, unbounded};

use crate::processing::block::Block;
use crate::processing::plan::PlanStage;
use crate::processing::sender::Sender;
use crate::processing::train::Train;
use crate::processing::transform::Transform;
use crate::processing::window::Window;
use crate::util::GLOBAL_ID;

pub(crate) struct Station {
    id: i64,
    pub stop: i64,
    incoming: (channel::Sender<Train>, Receiver<Train>),
    outgoing: Arc<Sender>,
    window: Window,
    pub(crate) transform: &'static Transform,
    block: Vec<i64>,
    inputs: Vec<i64>,
    control: (channel::Sender<Command>, Receiver<Command>),
}


impl Clone for Station {
    fn clone(&self) -> Self {
        Station {
            id: GLOBAL_ID.new_id(),
            stop: self.stop,
            incoming: (self.incoming.0.clone(), self.incoming.1.clone()),
            outgoing: Arc::clone(&self.outgoing),
            window: self.window.clone(),
            transform: self.transform.clone(),
            block: self.block.clone(),
            inputs: self.inputs.clone(),
            control: (self.control.0.clone(), self.control.1.clone()),
        }
    }
}


impl<'a> Default for Station {
    fn default() -> Self {
        Self::new(-1)
    }
}


impl Station {
    pub(crate) fn new(stop: i64) -> Self {
        let incoming = unbounded();
        let control = unbounded();
        let station = Station {
            id: GLOBAL_ID.new_id(),
            stop,
            incoming: (incoming.0, incoming.1),
            outgoing: Arc::new(Sender::default()),
            window: Window::default(),
            transform: &Transform::default(),
            block: vec![],
            inputs: vec![],
            control: (control.0.clone(), control.1.clone()),
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

    pub(crate) fn add_insert(&mut self, input: i64) {
        self.inputs.push(input);
    }

    pub(crate) fn merge(&mut self, other: Station) {
        for line in other.block {
            self.block.push(line)
        }
    }

    pub(crate) fn close(&mut self) {
        self.control.0.send(Command::STOP).expect(todo!());
    }

    pub(crate) fn set_stop(&mut self, stop: i64) {
        self.stop = stop
    }

    pub(crate) fn set_window(&mut self, window: Window) {
        self.window = window;
    }

    pub(crate) fn set_transform(&mut self, transform: Transform) {
        self.transform = &transform;
    }

    pub(crate) fn add_block(&mut self, line: i64) {
        self.block.push(line);
    }

    pub(crate) fn add_out(&mut self, id: i64, out: channel::Sender<Train>) -> Result<(), String> {
        self.outgoing.add(id, out);
        Ok(())
    }

    pub(crate) fn send(&mut self, train: Train) -> Result<(), String> {
        self.incoming.0.send(train).map_err(|e| e.to_string())
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

    pub(crate) fn get_in(&mut self) -> channel::Sender<Train> {
        self.incoming.0.clone()
    }

    pub(crate) fn operate(&mut self) -> JoinHandle<()> {
        let receiver = self.incoming.1.clone();
        let sender = self.outgoing.clone();
        let transform = self.transform.transformer();
        let window = self.window.windowing();
        let stop = self.stop;
        let blocks = self.block.clone();
        let inputs = self.inputs.clone();

        thread::spawn(move || {
            let process = move |trains: &mut Vec<Train>| {
                let mut transformed = transform.process(stop, window(trains));
                transformed.last = stop;
                sender.send(transformed)
            };
            let mut block = Block::new(inputs, blocks, Box::new(process));

            while let Ok(train) = receiver.recv() {
                block.next(train) // window takes precedence to
            }
        })
    }
}

pub(crate) enum Command {
    STOP,
    READY,
}


#[cfg(test)]
mod tests {
    use crossbeam::channel::unbounded;

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


        let (tx, rx) = unbounded();

        station.add_out(0, tx).unwrap();
        station.operate();
        station.send(Train::new(0, values.clone())).unwrap();

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

        let (output_tx, output_rx) = unbounded();

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