use core::default::Default;
use std::collections::HashMap;
use std::sync::Arc;

use crossbeam::channel;
use crossbeam::channel::{Sender, unbounded};
use serde::{Serialize, Serializer};
use serde::ser::SerializeStruct;
use crate::processing::destination::Destination;
use crate::processing::plan::PlanStage::{LayoutStage, Num, TransformStage, WindowStage, BlockStage};
use crate::processing::source::Source;
use crate::processing::station::{Command, Station};
use crate::util::GLOBAL_ID;

pub struct Plan {
    pub id: i64,
    name: String,
    lines: HashMap<i64, Vec<i64>>,
    pub(crate) stations: HashMap<i64, Station>,
    sources: HashMap<i64, Box<dyn Source>>,
    destinations: HashMap<i64, Box<dyn Destination>>,
    controls: HashMap<i64, Vec<Sender<Command>>>,
    control_receiver: (Arc<Sender<Command>>, channel::Receiver<Command>),
}

impl Default for Plan {
    fn default() -> Self {
        let (tx, rx) = unbounded();
        Plan {
            id: GLOBAL_ID.new_id(),
            name: "".to_string(),
            lines: Default::default(),
            stations: Default::default(),
            sources: Default::default(),
            destinations: Default::default(),
            controls: Default::default(),
            control_receiver: (Arc::new(tx), rx),
        }
    }
}


impl Plan {
    fn new(id: i64) -> Self {
        Plan {
            id,
            name: id.to_string(),
            ..Default::default()
        }
    }

    pub(crate) fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub(crate) fn halt(&mut self) {
        for (_, station) in &mut self.stations {
            station.close();
        }
    }

    pub(crate) fn dump(&self) -> String {
        let mut dump = "".to_string();
        let mut lines: Vec<(&i64, &Vec<i64>)> = self.lines.iter().collect();
        lines.sort_by_key(|&(key, _)| key);
        for line in lines {
            if *line.0 != 0 {
                dump += "\n"
            }

            let mut last = -1;
            for stop in line.1.iter().enumerate() {
                if stop.0 != 0 {
                    dump += "-";
                }
                dump += &self.stations[stop.1].dump(last);
                last = *stop.1
            }
        }

        dump
    }

    pub(crate) fn send_control(&mut self, num: &i64, command: Command) {
        self.controls.get_mut(num).unwrap_or(&mut Vec::new()).iter().for_each(|c| c.send(command.clone()).unwrap())
    }

    pub(crate) fn operate(&mut self) {
        self.connect_stops().unwrap();
        self.connect_destinations().unwrap();
        self.connect_sources().unwrap();
        for station in &mut self.stations {
            self.controls.entry(station.1.id).or_insert_with(Vec::new).push(station.1.operate(Arc::clone(&self.control_receiver.0)));
        }

        // wait for all stations to be ready
        let mut readys = vec![];
        while readys.len() != self.controls.len() {
            match self.control_receiver.1.recv() {
                Ok(command) => {
                    match command {
                        Command::READY(id) => { readys.push(id) }
                        _ => todo!()
                    }
                }
                _ => todo!()
            }
        }


        for destination in &mut self.destinations {
            self.controls.entry(destination.1.get_id()).or_insert_with(Vec::new).push(destination.1.operate(Arc::clone(&self.control_receiver.0)));
        }

        for source in &mut self.sources {
            self.controls.entry(source.1.get_id()).or_insert_with(Vec::new).push(source.1.operate(Arc::clone(&self.control_receiver.0)));
        }
    }

    pub(crate) fn clone_platform(&mut self, num: i64) {
        let station = self.stations.get_mut(&num).unwrap();
        self.controls.entry(num).or_insert_with(Vec::new).push(station.operate(Arc::clone(&self.control_receiver.0)))
    }

    fn connect_stops(&mut self) -> Result<(), String> {
        for (line, stops) in &self.lines {
            let mut stops_iter = stops.iter();

            if let Some(first) = stops_iter.next() {
                let mut last_station = first.clone();

                for num in stops_iter {
                    let next_station = self.stations.get_mut(num).ok_or("Could not find target station".to_string())?;
                    let next_stop_id = next_station.stop.clone();

                    next_station.add_insert(last_station);

                    let send = next_station.get_in();
                    let last = self.stations.get_mut(&last_station).ok_or("Could not find target station".to_string())?;
                    last.add_out(*line, send)?;

                    last_station = next_stop_id;
                }
            }
        }
        Ok(())
    }


    pub(crate) fn parse(stencil: &str) -> Self {
        let mut plan = Plan::default();

        let lines = stencil.split("\n");
        for line in lines.enumerate() {
            plan.parse_line(line.0 as i64, line.1);
        }

        plan
    }
    fn parse_line(&mut self, line: i64, stencil: &str) {
        let mut temp = "".to_string();
        let mut stage = Num;
        let mut current: Vec<(PlanStage, String)> = vec![];
        let mut is_text = false;

        for char in stencil.chars() {
            if is_text && char != '"' {
                temp.push(char);
                continue;
            }

            match char {
                '-' => {
                    match stage {
                        Num => current.push((stage, temp.clone())),
                        _ => {}
                    };

                    let station = Station::parse(self.lines.get(&line).map(|vec: &Vec<i64>| vec.last().cloned()).flatten(), current.clone());
                    self.build(line, station);
                    current.clear();
                    temp = "".to_string();
                    stage = Num;
                }
                '{' | '(' | '[' => {
                    match stage {
                        Num => {
                            current.push((stage, temp.clone()));
                            temp = "".to_string();
                        }
                        _ => {}
                    };
                    match char {
                        '[' => stage = WindowStage,
                        '(' => stage = LayoutStage,
                        '{' => stage = TransformStage,
                        _ => {}
                    }
                }
                '}' | ')' | ']' => {
                    current.push((stage, temp.clone()));
                    temp = "".to_string();
                }
                '"' => {
                    is_text = !is_text;
                    temp.push(char);
                }
                _ => {
                    if let Num = stage {
                        if char == '|' {
                            current.push((BlockStage, "".to_string()));
                            continue;
                        }
                    }

                    temp.push(char);
                }
            }
        }
        if !temp.is_empty() {
            current.push((stage, temp.clone()));
        }
        if !current.is_empty() {
            let station = Station::parse(self.lines.get(&line).map(|vec: &Vec<i64>| vec.last().cloned()).flatten(), current.clone());
            self.build(line, station);
        }
    }
    fn build(&mut self, line_num: i64, station: Station) {
        self.lines.entry(line_num).or_insert_with(Vec::new).push(station.stop);
        let stop = station.stop;
        let station = match self.stations.remove(&stop) {
            None => station,
            Some(mut other) => {
                other.merge(station);
                other
            }
        };
        self.stations.insert(station.stop, station);
    }

    fn build_split(&mut self, line_num: i64, stop_num: i64) -> Result<(), String> {
        self.lines.entry(line_num).or_insert_with(Vec::new).push(stop_num);
        Ok(())
    }

    fn connect_destinations(&mut self) -> Result<(), String> {
        for destination in &mut self.destinations {
            let tx = destination.1.get_in();
            let target = destination.1.get_stop();
            if let Some(station) = self.stations.get_mut(&target) {
                station.add_out(-1, tx)?;
            } else {
                todo!()
            }
        }
        Ok(())
    }
    fn connect_sources(&mut self) -> Result<(), String> {
        for source in &mut self.sources {
            let target = source.1.get_stop();
            if let Some(station) = self.stations.get_mut(&target) {
                let tx = station.get_in();
                source.1.add_out(station.stop, tx)
            }
        }
        Ok(())
    }

    fn add_source(&mut self, stop: i64, source: Box<dyn Source>) {
        self.sources.insert(stop, source);
    }

    fn add_destination(&mut self, stop: i64, destination: Box<dyn Destination>) {
        self.destinations.insert(stop, destination);
    }
}


#[derive(Clone, Copy)]
pub(crate) enum PlanStage {
    WindowStage,
    TransformStage,
    LayoutStage,
    BlockStage,
    Num,
}


impl Serialize for &Plan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Plan", 3)?;
        state.serialize_field("name", &self.name)?;

        let mut lines = HashMap::new();
        for (num, stops) in &self.lines {
            lines.insert(
                num.to_string(),
                Line {
                    num: *num,
                    stops: stops.clone(),
                },
            );
        }

        state.serialize_field("lines", &lines)?;

        let mut stops = HashMap::new();
        for (num, stop) in &self.stations {
            stops.insert(
                num.to_string(),
                Stop {
                    num: *num,
                    transform: stop.transform.get(num).map( |i| i.dump().clone()).unwrap_or("".to_string()),
                },
            );
        }

        state.serialize_field("stops", &stops)?;
        state.end()
    }
}

#[derive(Serialize)]
struct Line {
    num: i64,
    stops: Vec<i64>,
}

#[derive(Serialize)]
struct Stop {
    num: i64,
    transform: String,
}


#[cfg(test)]
mod test {
    use crate::processing::plan::Plan;

    #[test]
    fn parse_line_stop_stencil() {
        let stencils = vec![
            "1",
            "1-2",
            "1-2-3",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil);
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn parse_multiline_stop_stencil() {
        let stencils = vec![
            "1-2\
            3-2",
            "1-2-3\
            4-3",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil);
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn stencil_transform_sql() {
        let stencils = vec![
            "1-2{sql|SELECT * FROM $1}",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil);
            assert_eq!(plan.dump(), stencil)
        }
    }

    //#[test]
    fn stencil_transform_mql() {
        let stencils = vec![
            "1-2{sql|db.$1.find({})}",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil);
            assert_eq!(plan.dump(), stencil)
        }
    }


    #[test]
    fn stencil_window() {
        let stencils = vec![
            "1-2[3s]",
            "1-2[3s@13:15]",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil);
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn stencil_block() {
        let stencils = vec![
            "1-2-3\n4-|2",
            "1-|2-3\n4-2",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil);
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn stencil_branch() {
        let stencils = vec![
            "1-2{sql|SELECT $1.name FROM $1}\n1-3{sql|SELECT $1.age FROM $1}",
            /*"1-2{sql|$1 HAS name}\n1-3{sql|SELECT $1.age FROM $1}",
            "1-2{sql|$1 HAS NOT name}\n1-3{sql|SELECT $1.age FROM $1}",
            //
            "1-2{mql|db.$1.has(name: 1)}\n1-3{db.$1.find({},{age:1}}",*/
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil);
            assert_eq!(plan.dump(), stencil)
        }
    }
}

#[cfg(test)]
mod dummy {
    use std::sync::{Arc, Mutex};
    use std::thread::{sleep, spawn};
    use std::time::Duration;

    use crossbeam::channel::{Sender, unbounded};

    use crate::processing::destination::Destination;
    use crate::processing::source::Source;
    use crate::processing::station::Command;
    use crate::processing::station::Command::{READY, STOP};
    use crate::processing::train::Train;
    use crate::util::{GLOBAL_ID, new_channel, Rx, Tx};
    use crate::value::Value;

    pub struct DummySource {
        id: i64,
        stop: i64,
        values: Option<Vec<Vec<Value>>>,
        delay: Duration,
        initial_delay: Duration,
        senders: Option<Vec<Tx<Train>>>,
    }

    impl DummySource {
        pub(crate) fn new(stop: i64, values: Vec<Vec<Value>>, delay: Duration) -> Self {
            Self::new_with_delay(stop, values, Duration::from_millis(0), delay)
        }

        pub(crate) fn new_with_delay(stop: i64, values: Vec<Vec<Value>>, initial_delay: Duration, delay: Duration) -> Self {
            DummySource { id: GLOBAL_ID.new_id(), stop, values: Some(values), initial_delay, delay, senders: Some(vec![]) }
        }
    }

    impl Source for DummySource {
        fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
            let stop = self.stop;

            let delay = self.delay;
            let initial_delay = self.initial_delay;
            let values = self.values.take().unwrap();
            let senders = self.senders.take().unwrap();
            let (tx, rx) = unbounded();

            spawn(move || {
                control.send(READY(stop)).unwrap();
                // wait for ready from callee
                match rx.recv() {
                    Ok(command) => {
                        match command {
                            READY(id) => {}
                            _ => panic!()
                        }
                    }
                    _ => panic!()
                }
                sleep(initial_delay);


                for values in &values {
                    for sender in &senders {
                        sender.send(Train::new(0, values.clone())).unwrap();
                    }
                    sleep(delay);
                }
                control.send(STOP(stop)).unwrap()
            });
            tx
        }


        fn add_out(&mut self, id: i64, out: Tx<Train>) {
            self.senders.as_mut().unwrap_or(&mut vec![]).push(out)
        }

        fn get_stop(&self) -> i64 {
            self.stop
        }

        fn get_id(&self) -> i64 {
            self.id
        }
    }

    pub(crate) struct DummyDestination {
        id: i64,
        stop: i64,
        result_amount: usize,
        pub(crate) results: Arc<Mutex<Vec<Train>>>,
        receiver: Option<Rx<Train>>,
        sender: Tx<Train>,
    }

    impl DummyDestination {
        pub(crate) fn new(stop: i64, wait_result: usize) -> Self {
            let (tx, num, rx) = new_channel();
            DummyDestination {
                id: GLOBAL_ID.new_id(),
                stop,
                result_amount: wait_result,
                results: Arc::new(Mutex::new(vec![])),
                receiver: Some(rx),
                sender: tx,
            }
        }
    }

    impl Destination for DummyDestination {
        fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
            let stop = self.stop;
            let local = Arc::clone(&self.results);
            let receiver = self.receiver.take().unwrap();
            let result_amount = self.result_amount as usize;
            let (tx, rx) = unbounded();

            spawn(move || {
                control.send(READY(stop)).unwrap();
                let mut shared = local.lock().unwrap();
                loop {
                    match rx.try_recv() {
                        Ok(command) => match command {
                            STOP(_) => break,
                            _ => {}
                        },
                        _ => {}
                    }
                    match receiver.try_recv() {
                        Ok(train) => {
                            shared.push(train);
                            if shared.len() == result_amount {
                                break;
                            }
                        }
                        _ => sleep(Duration::from_nanos(100))
                    }
                }
                drop(shared);
                control.send(STOP(stop))
            });
            tx
        }

        fn get_in(&self) -> Tx<Train> {
            self.sender.clone()
        }

        fn get_stop(&self) -> i64 {
            self.stop
        }

        fn get_id(&self) -> i64 {
            self.id
        }
    }
}


#[cfg(test)]
mod stencil {
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::{Duration, SystemTime};
    use std::vec;

    use crate::processing::destination::Destination;
    use crate::processing::plan::dummy::{DummyDestination, DummySource};
    use crate::processing::plan::Plan;
    use crate::processing::source::Source;
    use crate::processing::station::Command::{READY, STOP};
    use crate::processing::station::Station;
    use crate::processing::Train;
    use crate::processing::transform::{FuncTransform, Transform};
    use crate::util::new_channel;
    use crate::value::Value;

    #[test]
    fn station_plan_train() {
        let values = vec![3.into(), "test".into()];

        let mut plan = Plan::default();
        let mut first = Station::new(0);
        let input = first.get_in();

        let (output_tx, nums, output_rx) = new_channel();

        let mut second = Station::new(1);
        second.add_out(0, output_tx).unwrap();

        plan.build(0, first);
        plan.build(0, second);

        plan.operate();

        input.send(Train::new(0, values.clone())).unwrap();

        let mut res = output_rx.recv().unwrap();
        assert_eq!(res.values.clone().unwrap(), values);
        assert_ne!(res.values.take().unwrap(), vec![Value::null()]);

        assert!(output_rx.try_recv().is_err());


        drop(input); // close the channel
        plan.halt()
    }

    #[test]
    fn station_plan_split_train() {
        let values = vec![3.into(), "test".into(), true.into(), Value::null()];

        let mut plan = Plan::default();
        let mut first = Station::new(0);
        let first_id = first.stop;
        let input = first.get_in();

        let (output1_tx, num, output1_rx) = new_channel();

        let (output2_tx, num, output2_rx) = new_channel();

        let mut second = Station::new(1);
        second.add_out(0, output1_tx).unwrap();

        let mut third = Station::new(2);
        third.add_out(0, output2_tx).unwrap();

        plan.build(0, first);
        plan.build(0, second);
        plan.build_split(1, first_id).unwrap();
        plan.build(1, third);

        plan.operate();

        input.send(Train::new(0, values.clone())).unwrap();

        let mut res = output1_rx.recv().unwrap();
        assert_eq!(res.values.clone().unwrap(), values);
        assert_ne!(res.values.take().unwrap(), vec![Value::null()]);

        assert!(output1_rx.try_recv().is_err());

        let mut res = output2_rx.recv().unwrap();
        assert_eq!(res.values.clone().unwrap(), values);
        assert_ne!(res.values.take().unwrap(), vec![Value::null()]);

        assert!(output2_rx.try_recv().is_err());


        drop(input); // close the channel
        plan.halt()
    }


    #[test]
    fn sql_parse_transform() {
        let values = vec![vec![3.into(), "test".into(), true.into(), Value::null()]];
        let stencil = "3{sql|Select * From $0}";

        let mut plan = Plan::parse(stencil);

        let source = DummySource::new(3, values.clone(), Duration::from_millis(3));
        let id = &source.get_id();

        let destination = DummyDestination::new(3, values.len());
        let clone = Arc::clone(&destination.results);

        plan.add_source(3, Box::new(source));
        plan.add_destination(3, Box::new(destination));


        plan.operate();

        // start dummy source
        plan.send_control(id, READY(3));

        // source ready + stop, destination ready + stop
        for _command in vec![READY(3), STOP(3), READY(3), STOP(3)] {
            plan.control_receiver.1.recv().unwrap();
        }


        let results = clone.lock().unwrap();
        for mut train in results.clone() {
            assert_eq!(train.values.take().unwrap(), *values.get(0).unwrap())
        }
    }

    #[test]
    fn sql_parse_block_one() {
        let stencil = "1-|2-3\n4-2";


        let mut plan = Plan::parse(stencil);
        let values1 = vec![vec![3.3.into()], vec![3.1.into()]];
        let source1 = DummySource::new(1, values1.clone(), Duration::from_millis(1));
        let id1 = &source1.get_id().clone();

        let values4 = vec![vec![3.into()]];
        let source4 = DummySource::new_with_delay(4, values4.clone(), Duration::from_millis(3), Duration::from_millis(1));
        let id4 = &source4.get_id().clone();

        let destination = DummyDestination::new(3, 1);
        let id3 = &destination.get_id();
        let clone = Arc::clone(&destination.results);

        plan.add_source(1, Box::new(source1));
        plan.add_source(4, Box::new(source4));
        plan.add_destination(3, Box::new(destination));


        plan.operate();

        // send ready
        plan.send_control(id1, READY(0));
        plan.send_control(id4, READY(4));

        // source 1 ready + stop, source 4 ready + stop, destination ready + stop
        for com in vec![READY(1), STOP(1), READY(4), STOP(4), READY(3), STOP(3)] {
            match plan.control_receiver.1.recv() {
                Ok(command) => {}
                Err(_) => panic!()
            }
        }

        let mut res = vec![];

        values1.into_iter().for_each(|mut values| res.append(&mut values));
        values4.into_iter().for_each(|mut values| res.append(&mut values));

        let lock = clone.lock().unwrap();
        let mut train = lock.clone().pop().unwrap();
        drop(lock);

        assert_eq!(train.values.clone().unwrap().len(), res.len());
        for (i, value) in train.values.take().unwrap().into_iter().enumerate() {
            assert!(res.contains(&value))
        }
    }

    #[test]
    fn divide_workload() {
        let mut station = Station::new(0);
        let station_id = station.id;
        station.set_transform(0, Transform::Func(FuncTransform::new_boxed(|num, train| {
            sleep(Duration::from_millis(10));
            Train::from(train)
        })));

        let mut values = vec![];

        let numbers = 0..1_000;
        let length = numbers.len();

        for num in numbers {
            values.push(vec![Value::int(3)]);
        }

        let mut plan = Plan::new(0);

        let source = DummySource::new(0, values, Duration::from_nanos(3));
        let id = &source.get_id();


        plan.build(0, station);

        plan.add_source(0, Box::new(source));

        let destination = DummyDestination::new(0, length);
        plan.add_destination(0, Box::new(destination));

        plan.operate();
        let now = SystemTime::now();
        plan.send_control(id, READY(0));
        plan.clone_platform(0);
        plan.clone_platform(0);
        plan.clone_platform(0);


        // source 1 ready + stop, each platform ready, destination ready (+ stop only after stopped)
        for com in vec![READY(1), STOP(1), READY(0), READY(0), READY(0), READY(0), READY(0)] {
            match plan.control_receiver.1.recv() {
                Ok(command) => {}
                Err(_) => panic!()
            }
        }


        println!("time: {} millis", now.elapsed().unwrap().as_millis())
    }
}