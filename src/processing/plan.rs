use core::default::Default;
use crossbeam::channel;
use crossbeam::channel::{unbounded, Sender};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::Arc;

use crate::processing;
use crate::processing::destination::Destination;
use crate::processing::plan::Status::Stopped;
use crate::processing::source::Source;
use crate::processing::station::{Command, Station};
use crate::ui::ConfigModel::StringConf;
use crate::ui::{ConfigContainer, ConfigModel, StringModel};
use crate::util::GLOBAL_ID;

pub struct Plan {
    pub id: i64,
    pub name: String,
    lines: HashMap<i64, Vec<i64>>,
    pub(crate) stations: HashMap<i64, Station>,
    sources: HashMap<i64, Vec<Box<dyn Source>>>,
    destinations: HashMap<i64, Vec<Box<dyn Destination>>>,
    controls: HashMap<i64, Vec<Sender<Command>>>,
    pub(crate) control_receiver: (Arc<Sender<Command>>, channel::Receiver<Command>),
    pub(crate) status: Status,
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
            status: Stopped,
        }
    }
}

#[derive(Clone)]
pub enum Status {
    Running,
    Stopped,
    Error,
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
                        Command::Ready(id) => {
                            readys.push(id);
                        }
                        _ => todo!()
                    }
                }
                _ => todo!()
            }
        }


        for (_stop, destinations) in &mut self.destinations {
            for destination in destinations {
                self.controls.entry(destination.get_id()).or_default().push(destination.operate(Arc::clone(&self.control_receiver.0)));
            }
        }

        for (_stop, sources) in &mut self.sources {
            for source in sources {
                self.controls.entry(source.get_id()).or_default().push(source.operate(Arc::clone(&self.control_receiver.0)));
            }
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
                    let station = Station::parse(temp.clone(), last);
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
        for (_stop, destinations) in &mut self.destinations {
            for destination in destinations {
                let tx = destination.get_in();
                let target = destination.get_stop();
                if let Some(station) = self.stations.get_mut(&target) {
                    station.add_out(-1, tx)?;
                } else {
                    todo!()
                }
            }
        }
        Ok(())
    }
    fn connect_sources(&mut self) -> Result<(), String> {
        for (_stop, sources) in &mut self.sources {
            for source in sources {
                let target = source.get_stop();
                if let Some(station) = self.stations.get_mut(&target) {
                    let tx = station.get_in();
                    source.add_out(station.stop, tx)
                }
            }
        }
        Ok(())
    }

    pub(crate) fn add_source(&mut self, stop: i64, source: Box<dyn Source>) {
        assert_eq!(stop, source.get_stop());
        self.sources.entry(stop).or_default().push(source);
    }

    pub(crate) fn add_destination(&mut self, stop: i64, destination: Box<dyn Destination>) {
        assert_eq!(stop, destination.get_stop());
        self.destinations.entry(stop).or_default().push(destination);
    }
}


#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PlanStage {
    Window,
    Transform,
    Layout,
    Num,
}

pub(crate) struct Stage {
    pub(crate) open: char,
    pub(crate) close: char,
}

impl PlanStage {
    pub(crate) const WINDOW_OPEN: char = '[';
    pub(crate) const WINDOW_CLOSE: char = ']';
    pub(crate) const TRANSFORM_OPEN: char = '{';
    pub(crate) const TRANSFORM_CLOSE: char = '}';
    pub(crate) const LAYOUT_OPEN: char = '(';
    pub(crate) const LAYOUT_CLOSE: char = ')';

    pub(crate) fn is_open(char: char) -> bool {
        matches!(char, PlanStage::LAYOUT_OPEN | PlanStage::TRANSFORM_OPEN | PlanStage::WINDOW_OPEN)
    }

    pub(crate) fn is_close(char: char) -> bool {
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
        state.serialize_field("id", &self.id)?;

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
                    }),
                    sources: self.sources.get(&stop.stop).unwrap_or(&vec![]).iter().map(|s| s.serialize()).collect(),
                    destinations: self.destinations.get(&stop.stop).unwrap_or(&vec![]).iter().map(|d| d.serialize()).collect(),
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

#[derive(Serialize, Deserialize)]
struct Stop {
    num: i64,
    transform: Option<ConfigContainer>,
    sources: Vec<SourceModel>,
    destinations: Vec<DestinationModel>,
}


#[derive(Serialize, Deserialize)]
pub struct SourceModel {
    pub(crate) type_name: String,
    pub(crate) id: String,
    pub(crate) configs: HashMap<String, ConfigModel>,
}

#[derive(Serialize, Deserialize)]
pub struct DestinationModel {
    pub(crate) type_name: String,
    pub(crate) id: String,
    pub(crate) configs: HashMap<String, ConfigModel>,
}

#[derive(Serialize, Deserialize)]
struct Transform {
    language: String,
    query: String,
}


impl From<processing::transform::Transform> for ConfigContainer {
    fn from(value: processing::transform::Transform) -> Self {
        match value {
            processing::transform::Transform::Func(_) => {
                let mut map = HashMap::new();
                map.insert(String::from("type"), StringConf(StringModel::new("Function")));
                map.insert(String::from("query"), StringConf(StringModel::new("")));
                ConfigContainer::new(String::from("Transform"), map)
            }
            processing::transform::Transform::Lang(l) => {
                let mut map = HashMap::new();
                map.insert(String::from("language"), StringConf(StringModel::new(&l.language.to_string())));
                map.insert(String::from("query"), StringConf(StringModel::new(&l.query.to_string())));
                ConfigContainer::new(String::from("Transform"), map)
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

