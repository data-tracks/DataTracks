use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::thread;

use crossbeam::channel;
use crossbeam::channel::{Receiver, unbounded};
use PlanStage::BlockStage;
use crate::processing::layout::Field;
use crate::processing::plan::PlanStage;
use crate::processing::plan::PlanStage::{LayoutStage, Num, TransformStage, WindowStage};
use crate::processing::platform::Platform;
use crate::processing::sender::Sender;
use crate::processing::train::Train;
use crate::processing::transform::Transform;
use crate::processing::window::Window;
use crate::util::{GLOBAL_ID, new_channel, Rx, Tx};

pub(crate) struct Station {
    pub(crate) id: i64,
    pub stop: i64,
    pub(crate) incoming: (Tx<Train>, Arc<AtomicU64>, Rx<Train>),
    pub(crate) outgoing: Sender,
    pub(crate) window: Window,
    pub(crate) transform: HashMap<i64, Transform>,
    pub(crate) block: Vec<i64>,
    pub(crate) inputs: Vec<i64>,
    pub(crate) layout: Field,
    control: (channel::Sender<Command>, Receiver<Command>),
}


impl Default for Station {
    fn default() -> Self {
        Self::new(-1)
    }
}


impl Station {
    pub(crate) fn new(stop: i64) -> Self {
        let incoming = new_channel();
        let control = unbounded();
        Station {
            id: GLOBAL_ID.new_id(),
            stop,
            incoming: (incoming.0, incoming.1, incoming.2),
            outgoing: Sender::default(),
            window: Window::default(),
            transform: HashMap::default(),
            block: vec![],
            inputs: vec![],
            layout: Field::default(),
            control: (control.0.clone(), control.1.clone()),
        }
    }

    pub(crate) fn parse(stencil: String, last: Option<i64>) -> Self{
        let mut temp = String::default();
        let mut is_text = false;
        let mut stages = vec![];
        let mut current_stage = Some(Num);

        for char in stencil.chars() {
            if char != '"' && is_text {
                temp.push(char);
                continue;
            }

            // can we end number
            if let Some(_stage @ Num) = current_stage {
                match char {
                    '(' | '{' | '[' => {
                        stages.push((Num, temp.clone()));
                        temp = String::default();
                        current_stage = None;
                    }
                    _ => {}
                }
            }

            match char {
                '"' => {
                    if is_text {
                        is_text = false;
                    }else {
                        is_text = true;
                    }
                }
                '(' | '{' | '[' => {
                    if current_stage.is_some() {
                        temp.push(char);
                        continue;
                    }
                    current_stage = Some(match char {
                        '(' => LayoutStage,
                        '{' => TransformStage,
                        '[' => WindowStage,
                        _ => current_stage.unwrap()
                    })
                }
                c => {
                    if let Some(stage) = current_stage{
                        if (c == ')' && stage == LayoutStage)
                            || (c == '}' && stage == TransformStage)
                            || (c == ']' && stage == LayoutStage){
                            stages.push((stage, temp.clone()));
                            temp = String::default();
                            current_stage = None;
                            continue
                        }
                    }
                    temp.push(char);
                }
            }
        }
        if !temp.is_empty() {
            if let Some(stage @ Num) = current_stage {
                stages.push((stage, temp))
            }else {
                panic!("Not finished parsing")
            }
        }

        Self::parse_stages(last, stages )
    }

    pub(crate) fn parse_stages(last: Option<i64>, parts: Vec<(PlanStage, String)>) -> Self {
        let mut station: Station = Station::default();
        for stage in parts {
            match stage.0 {
                WindowStage => station.set_window(Window::parse(stage.1)),
                TransformStage => station.set_transform(last.unwrap_or(-1), Transform::parse(stage.1).unwrap()),
                BlockStage => station.add_block(last.unwrap_or(-1)),
                LayoutStage => station.add_explicit_output(Field::parse(stage.1)),
                Num => station.set_stop(stage.1.parse::<i64>().unwrap()),
            }
        }
        station
    }

    pub(crate) fn add_insert(&mut self, input: i64) {
        self.inputs.push(input);
    }

    pub(crate) fn merge(&mut self, mut other: Station) {
        self.block.append(other.block.as_mut());
        other.transform.into_iter().for_each(|(num, transform)| {
            self.transform.insert(num, transform.clone());
        })
    }

    pub(crate) fn close(&mut self) {
        self.control.0.send(Command::Stop(0)).expect("TODO: panic message");
    }

    pub(crate) fn set_stop(&mut self, stop: i64) {
        self.stop = stop
    }

    pub(crate) fn set_window(&mut self, window: Window) {
        self.window = window;
    }

    pub(crate) fn set_transform(&mut self, id: i64,  transform: Transform) {
        self.transform.insert(id, transform);
    }

    pub(crate) fn add_block(&mut self, line: i64) {
        self.block.push(line);
    }

    pub(crate) fn add_out(&mut self, id: i64, out: Tx<Train>) -> Result<(), String> {
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
        if let Some(transform) = self.transform.get(&line) {
            dump += &transform.dump();
        }
        dump
    }

    pub(crate) fn get_in(&mut self) -> Tx<Train> {
        self.incoming.0.clone()
    }

    pub(crate) fn operate(&mut self, control: Arc<channel::Sender<Command>>) -> channel::Sender<Command> {
        let (mut platform, sender) = Platform::new(self);

        thread::spawn(move || {
            platform.operate(control)
        });
        sender
    }

    fn add_explicit_output(&mut self, output: Field) {
        self.layout = output
    }
}


#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Command {
    Stop(i64),
    Ready(i64),
    Overflow(i64),
    Threshold(i64),
    Okay(i64),
}


#[cfg(test)]
pub mod tests {
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use crossbeam::channel::{Receiver, Sender, unbounded};
    use crate::processing::plan::Plan;
    use crate::processing::station::{Command, Station};
    use crate::processing::station::Command::{Okay, Ready, Threshold};
    use crate::processing::train::Train;
    use crate::processing::transform::{FuncTransform, Transform};
    use crate::util::{new_channel, Rx, Tx};
    use crate::value::{Dict, Value};
    use crate::processing::tests::dict_values;

    #[test]
    fn start_stop_test() {
        let mut station = Station::new(0);

        let control = unbounded();

        let mut values = dict_values(vec![Value::text("test"), Value::bool(true), Value::float(3.3), Value::null()]);

        for x in 0..1_000_000 {
            values.push(Dict::from(Value::int(x)))
        }


        let (tx, num, rx) = new_channel();

        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0));
        station.send(Train::new(0, values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(mut t) => {
                assert_eq!(values.len(), t.values.clone().map_or(usize::MAX, |values| values.len()));
                for (i, value) in t.values.take().unwrap().iter().enumerate() {
                    assert_eq!(value, &values[i]);
                    assert_ne!(&Value::text(""), value.0.get("$").unwrap())
                }
            }
            Err(..) => assert!(false),
        }
    }


    #[test]
    fn station_two_train() {
        let values = vec![Dict::from(Value::int(3))];
        let (tx, rx) = unbounded();
        let control = Arc::new(tx);

        let mut first = Station::new(0);
        let input = first.get_in();

        let (output_tx, num, output_rx) = new_channel();

        let mut second = Station::new(1);
        second.add_out(0, output_tx).unwrap();

        let tx = second.get_in();
        first.add_out(1, tx).unwrap();

        first.operate(Arc::clone(&control));
        second.operate(Arc::clone(&control));

        input.send(Train::new(0, values.clone())).unwrap();

        let res = output_rx.recv().unwrap();
        assert_eq!(res.values.clone().unwrap(), values);
        assert_ne!(res.values.clone().unwrap(), vec![Value::null().into()]);

        assert!(output_rx.try_recv().is_err());


        drop(input); // close the channel
        first.close();
        second.close();
    }

    #[test]
    fn sql_parse_block() {
        let stencil = "1-|3{sql|Select * From $1}";

        let plan = Plan::parse(stencil);

        let station = plan.stations.get(&3).unwrap();

        assert!(station.block.contains(&1));
    }

    #[test]
    fn sql_parse_different_outs() {
        /*let stencil = "1-3{sql|Select $1.0 From $1}";
        let stencil = "1-3{sql|Select $1.name From $1}";

        let plan = Plan::parse(stencil);

        let station = plan.stations.get(&3).unwrap();*/
    }

    #[test]
    fn sql_parse_output() {
        let stencils = vec![
            "1(f)",// scalar
            "1(d{name:t, temperature:f})", // named tuple
            "1(a(length: 3)i)" // array
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil);

            let station = plan.stations.get(&1).unwrap();
        }

    }

    #[test]
    fn too_high() {
        let (mut station, train_sender, rx, c_rx, a_tx) = create_test_station(10);

        let sender = station.operate(Arc::clone(&a_tx));
        sender.send(Threshold(3)).unwrap();

        for _ in 0..1_000 {
            train_sender.send(Train::new(0, dict_values(vec![Value::int(3)]))).unwrap();
        }

        // the station should start, the threshold should be reached and after some time be balanced
        for state in vec![Ready(0), Threshold(0), Okay(0)] {
            assert_eq!(state, c_rx.recv().unwrap())
        }
    }

    #[test]
    fn too_high_two() {
        let (mut station, train_sender, rx, c_rx, a_tx) = create_test_station(100);

        let sender = station.operate(Arc::clone(&a_tx));
        sender.send(Threshold(3)).unwrap();
        station.operate(Arc::clone(&a_tx));

        for _ in 0..1_000 {
            train_sender.send(Train::new(0, dict_values(vec![Value::int(3)]))).unwrap();
        }

        // the station should open a platform, the station starts another platform,  the threshold should be reached and after some time be balanced
        for state in vec![Ready(0), Ready(0), Threshold(0), Okay(0)] {
            assert_eq!(state, c_rx.recv().unwrap())
        }
    }

    #[test]
    fn remove_during_op() {
        let (mut station, train_sender, rx, c_rx, a_tx) = create_test_station(10);
        let sender = station.operate(Arc::clone(&a_tx));

        for _ in 0..500 {
            train_sender.send(Train::new(0, dict_values(vec![Value::int(3)]))).unwrap();
        }
        station.operate(Arc::clone(&a_tx));
        for _ in 0..500 {
            train_sender.send(Train::new(0, dict_values(vec![Value::int(3)]))).unwrap();
        }

        // the station should open a platform, the station starts another platform,  the threshold should be reached and after some time be balanced
        for state in vec![Ready(0), Ready(0)] {
            assert_eq!(state, c_rx.recv().unwrap())
        }
        let mut values = vec![];

        while values.len() < 1_000 {
            values.push(rx.recv().unwrap())
        }
        assert_eq!(values.len(), 1_000)
    }

    #[test]
    fn minimal_overhead() {
        let (mut station, train_sender, rx, c_rx, a_tx) = create_test_station(0);

        let sender = station.operate(Arc::clone(&a_tx));

        let mut trains = vec![];

        for _ in 0..1_000 {
            trains.push(Train::new(0, dict_values(vec![Value::int(3)])));
        }

        // the station should open a platform
        for state in vec![Ready(0)] {
            assert_eq!(state, c_rx.recv().unwrap())
        }
        let time = Instant::now();

        for train in trains {
            train_sender.send(train).unwrap();
        }

        let mut values: i32 = 0;

        while values < 1_000 {
            rx.recv().unwrap();
            values += 1;
        }
        let elapsed = time.elapsed().as_nanos();
        println!("time: {}nanos, per entry {}ns", elapsed, elapsed/1_000 );
    }

    fn create_test_station(duration: u64) -> (Station, Tx<Train>, Rx<Train>, Receiver<Command>, Arc<Sender<Command>>) {
        let mut station = Station::new(0);
        let train_sender = station.get_in();
        let (tx, _, rx) = new_channel();
        let train_receiver = station.add_out(0, tx);
        let time = duration.clone();


        station.set_transform(0,  match duration  {
            0 => {
                Transform::Func(FuncTransform::new(Arc::new(move |num, train| {
                    Train::from(train)
                })))
            },
            _ => {
                Transform::Func(FuncTransform::new(Arc::new(move |num, train| {
                    sleep(Duration::from_millis(time));
                    Train::from(train)
                })))
        } });

        let (c_tx, c_rx) = unbounded();

        let a_tx = Arc::new(c_tx);
        (station, train_sender, rx, c_rx, a_tx)
    }

}