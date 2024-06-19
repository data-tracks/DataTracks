use std::collections::HashMap;

use crate::processing::destination::Destination;
use crate::processing::plan::PlanStage::{BlockStage, Num, TransformStage, WindowStage};
use crate::processing::source::Source;
use crate::processing::station::Station;
use crate::util::GLOBAL_ID;

pub(crate) struct Plan {
    id: i64,
    lines: HashMap<i64, Vec<i64>>,
    pub(crate) stations: HashMap<i64, Station>,
    sources: HashMap<i64, Box<dyn Source>>,
    destinations: HashMap<i64, Box<dyn Destination>>,
}


impl Plan {
    pub(crate) fn default() -> Self {
        Plan::new(GLOBAL_ID.new_id())
    }

    fn new(id: i64) -> Self {
        Plan {
            id,
            lines: HashMap::new(),
            stations: HashMap::new(),
            sources: HashMap::new(),
            destinations: HashMap::new(),
        }
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

    pub(crate) fn operate(&mut self) {
        self.connect_stops().unwrap();
        self.connect_destinations().unwrap();
        self.connect_sources().unwrap();
        for station in &mut self.stations {
            station.1.operate();
        }

        for destination in &mut self.destinations {
            destination.1.operate();
        }

        for source in &mut self.sources {
            source.1.operate();
        }
    }

    fn connect_stops(&mut self) -> Result<(), String> {
        for (line, stops) in &self.lines {
            let mut stops_iter = stops.iter();

            if let Some(first) = stops_iter.next() {
                let mut last_station = first.clone();

                for num in stops_iter {
                    let next_station = self.stations.get_mut(num).ok_or("Could not find target station".to_string())?;
                    let next_stop_id = next_station.stop.clone();


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
                        '[' => stage = BlockStage,
                        '(' => stage = WindowStage,
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
    BlockStage,
    Num,
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
    fn stencil_transform() {
        let stencils = vec![
            "1-2{sql|SELECT * FROM $1}",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil);
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn stencil_window() {
        let stencils = vec![
            "1-2(3s)",
            "1-2(3s@13:15)",
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
}

#[cfg(test)]
mod dummy {
    use std::sync::{Arc, Mutex};
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::thread::{sleep, spawn};
    use std::time::Duration;

    use crate::processing::destination::Destination;
    use crate::processing::source::Source;
    use crate::processing::train::Train;
    use crate::value::Value;

    pub struct DummySource {
        stop: i64,
        values: Vec<Vec<Value>>,
        delay: Duration,
        senders: Vec<Sender<Train>>,
    }

    impl DummySource {
        pub(crate) fn new(stop: i64, values: Vec<Vec<Value>>, delay: Duration) -> Self {
            DummySource { stop, values, delay, senders: vec![] }
        }
    }

    impl Source for DummySource {
        fn operate(&self) {
            for values in &self.values {
                for sender in &self.senders {
                    sender.send(Train::default(values.clone())).unwrap();
                }
                sleep(self.delay);
            }
        }


        fn add_out(&mut self, id: i64, out: Sender<Train>) {
            self.senders.push(out)
        }

        fn get_stop(&self) -> i64 {
            self.stop
        }
    }

    pub(crate) struct DummyDestination {
        stop: i64,
        pub(crate) results: Arc<Mutex<Vec<Train>>>,
        receiver: Option<Receiver<Train>>,
        sender: Sender<Train>,
    }

    impl DummyDestination {
        pub(crate) fn new(stop: i64) -> Self {
            let (tx, rx) = channel();
            DummyDestination { stop, results: Arc::new(Mutex::new(vec![])), receiver: Some(rx), sender: tx }
        }
    }

    impl Destination for DummyDestination {
        fn operate(&mut self) {
            let local = Arc::clone(&self.results);
            let receiver = self.receiver.take().unwrap();
            spawn(move || {
                while let Ok(res) = receiver.recv() {
                    let mut shared = local.lock().unwrap();
                    shared.push(res);
                }
            });
        }

        fn get_in(&self) -> Sender<Train> {
            self.sender.clone()
        }

        fn get_stop(&self) -> i64 {
            self.stop
        }
    }
}

#[cfg(test)]
mod stencil {
    use std::sync::Arc;
    use std::sync::mpsc::channel;
    use std::thread::sleep;
    use std::time::Duration;

    use crate::processing::plan::dummy::{DummyDestination, DummySource};
    use crate::processing::plan::Plan;
    use crate::processing::station::Station;
    use crate::processing::Train;
    use crate::value::Value;

    #[test]
    fn station_plan_train() {
        let values = vec![3.into(), "test".into()];

        let mut plan = Plan::default();
        let mut first = Station::new(0);
        let input = first.get_in();

        let (output_tx, output_rx) = channel();

        let mut second = Station::new(1);
        second.add_out(0, output_tx).unwrap();

        plan.build(0, first);
        plan.build(0, second);

        plan.operate();

        input.send(Train::default(values.clone())).unwrap();

        let mut res = output_rx.recv().unwrap();
        assert_eq!(res.values.get(&0).unwrap().clone().unwrap(), values);
        assert_ne!(res.values.get_mut(&0).unwrap().take().unwrap(), vec![Value::null()]);

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

        let (output1_tx, output1_rx) = channel();

        let (output2_tx, output2_rx) = channel();

        let mut second = Station::new(1);
        second.add_out(0, output1_tx).unwrap();

        let mut third = Station::new(2);
        third.add_out(0, output2_tx).unwrap();

        plan.build(0, first);
        plan.build(0, second);
        plan.build_split(1, first_id).unwrap();
        plan.build(1, third);

        plan.operate();

        input.send(Train::default(values.clone())).unwrap();

        let mut res = output1_rx.recv().unwrap();
        assert_eq!(res.values.get(&0).unwrap().clone().unwrap(), values);
        assert_ne!(res.values.get_mut(&0).unwrap().take().unwrap(), vec![Value::null()]);

        assert!(output1_rx.try_recv().is_err());

        let mut res = output2_rx.recv().unwrap();
        assert_eq!(res.values.get(&0).unwrap().clone().unwrap(), values);
        assert_ne!(res.values.get_mut(&0).unwrap().take().unwrap(), vec![Value::null()]);

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

        let destination = DummyDestination::new(3);
        let clone = Arc::clone(&destination.results);

        plan.add_source(3, Box::new(source));
        plan.add_destination(3, Box::new(destination));


        plan.operate();

        loop {
            let shared = clone.lock().unwrap();
            if !shared.is_empty() {
                plan.halt();
                break;
            }
            drop(shared);
            sleep(Duration::from_millis(5))
        }
        let results = clone.lock().unwrap();
        for mut train in results.clone() {
            assert_eq!(train.values.get_mut(&0).unwrap().take().unwrap(), *values.get(0).unwrap())
        }
    }
}