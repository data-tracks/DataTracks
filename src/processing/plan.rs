use core::default::Default;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::Arc;

use crossbeam::channel;
use crossbeam::channel::{unbounded, Sender};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

use crate::processing;
use crate::processing::destination::Destination;
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
    pub(crate) control_receiver: (Arc<Sender<Command>>, channel::Receiver<Command>),
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
    pub fn new(id: i64) -> Self {
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
        for station in self.stations.values_mut() {
            station.close();
        }
    }

    pub(crate) fn dump(&self) -> String {
        let mut dump = "".to_string();
        let mut dumped_stations = vec![];
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
                dump += &self.stations[stop.1].dump(last, dumped_stations.contains(&stop.1.clone()));
                last = *stop.1;
                dumped_stations.push(stop.1.clone())
            }
        }

        dump
    }

    pub(crate) fn send_control(&mut self, num: &i64, command: Command) {
        self.controls.get_mut(num).unwrap_or(&mut Vec::new()).iter().for_each(|c| c.send(command.clone()).unwrap())
    }

    pub fn operate(&mut self) {
        self.connect_stops().unwrap();
        self.connect_destinations().unwrap();
        self.connect_sources().unwrap();
        for station in &mut self.stations {
            self.controls.entry(station.1.id).or_default().push(station.1.operate(Arc::clone(&self.control_receiver.0)));
        }

        // wait for all stations to be ready
        let mut readys = vec![];
        while readys.len() != self.controls.len() {
            match self.control_receiver.1.recv() {
                Ok(command) => {
                    match command {
                        Command::Ready(id) => { readys.push(id) }
                        _ => todo!()
                    }
                }
                _ => todo!()
            }
        }


        for destination in &mut self.destinations {
            self.controls.entry(destination.1.get_id()).or_default().push(destination.1.operate(Arc::clone(&self.control_receiver.0)));
        }

        for source in &mut self.sources {
            self.controls.entry(source.1.get_id()).or_default().push(source.1.operate(Arc::clone(&self.control_receiver.0)));
        }
    }

    pub(crate) fn clone_platform(&mut self, num: i64) {
        let station = self.stations.get_mut(&num).unwrap();
        self.controls.entry(num).or_default().push(station.operate(Arc::clone(&self.control_receiver.0)))
    }

    fn connect_stops(&mut self) -> Result<(), String> {
        for (line, stops) in &self.lines {
            let mut stops_iter = stops.iter();

            if let Some(first) = stops_iter.next() {
                let mut last_station = *first;

                for num in stops_iter {
                    let next_station = self.stations.get_mut(num).ok_or("Could not find target station".to_string())?;
                    let next_stop_id = next_station.stop;

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


    pub fn parse(stencil: &str) -> Self {
        let mut plan = Plan::default();

        let lines = stencil.split('\n');
        for line in lines.enumerate() {
            plan.parse_line(line.0 as i64, line.1);
        }

        plan
    }
    fn parse_line(&mut self, line: i64, stencil: &str) {
        let mut temp = String::default();
        let mut is_text = false;

        let mut last = None;

        for char in stencil.chars() {
            if is_text && char != '"' {
                temp.push(char);
                continue;
            }


            match char {
                '-' => {
                    let station = Station::parse( temp.clone(), last);
                    last = Some(station.stop);
                    self.build(line, station);

                    temp = String::default();
                }

                '"' => {
                    is_text = !is_text;
                    temp.push(char);
                }
                _ => {
                    temp.push(char);
                }
            }
        }

        if !temp.is_empty() {
            let station = Station::parse(temp, last);
            self.build(line, station);
        }
    }
    pub(crate) fn build(&mut self, line_num: i64, station: Station) {
        self.lines.entry(line_num).or_default().push(station.stop);
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

    pub(crate) fn build_split(&mut self, line_num: i64, stop_num: i64) -> Result<(), String> {
        self.lines.entry(line_num).or_default().push(stop_num);
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

    pub(crate) fn add_source(&mut self, stop: i64, source: Box<dyn Source>) {
        self.sources.insert(stop, source);
    }

    pub(crate) fn add_destination(&mut self, stop: i64, destination: Box<dyn Destination>) {
        self.destinations.insert(stop, destination);
    }
}


#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PlanStage {
    Window,
    Transform,
    Layout,
    Num,
}

pub(crate) struct Stage{
    pub(crate) open: char,
    pub(crate) close: char
}

impl PlanStage {
    pub(crate) const WINDOW_OPEN: char = '[';
    pub(crate) const WINDOW_CLOSE: char = ']';
    pub(crate) const TRANSFORM_OPEN: char = '{';
    pub(crate) const TRANSFORM_CLOSE: char = '}';
    pub(crate) const LAYOUT_OPEN: char = '(';
    pub(crate) const LAYOUT_CLOSE: char = ')';

    pub(crate) fn is_open(char: char) -> bool{
        matches!(char, PlanStage::LAYOUT_OPEN | PlanStage::TRANSFORM_OPEN | PlanStage::WINDOW_OPEN)
    }

    pub(crate) fn is_close(char: char) -> bool{
        matches!(char, PlanStage::LAYOUT_CLOSE | PlanStage::TRANSFORM_CLOSE | PlanStage::WINDOW_CLOSE)
    }
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
                    transform: stop.transform.clone().map(|t| {
                        t.into()
                    })
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
    transform: Option<Transform>,
}

#[derive(Serialize)]
struct Transform {
    language: String,
    query: String,
}


impl From<processing::transform::Transform> for Transform {
    fn from(value: processing::transform::Transform) -> Self {
        match value {
            processing::transform::Transform::Func(_) => {
                Transform { language: "Function".to_string(), query: "".to_string() }
            }
            processing::transform::Transform::Lang(l) => {
                Transform { language: l.language.to_string(), query: l.query }
            }
        }
    }
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

