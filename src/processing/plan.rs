use crate::processing::destination::{parse_destination, Destination};
use crate::processing::plan::Status::Stopped;
use crate::processing::source::{parse_source, Source};
use crate::processing::station::{Command, Station};
use crate::processing::{transform, Train};
use crate::ui::{ConfigContainer, ConfigModel, StringModel};
use crate::util::GLOBAL_ID;
use core::default::Default;
use crossbeam::channel;
use crossbeam::channel::{unbounded, Sender};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Plan {
    pub id: i64,
    pub name: String,
    pub lines: HashMap<i64, Vec<i64>>,
    pub(crate) stations: HashMap<i64, Station>,
    pub stations_to_in_outs: HashMap<i64, Vec<i64>>,
    pub sources: HashMap<i64, Box<dyn Source>>,
    pub destinations: HashMap<i64, Box<dyn Destination>>,
    pub controls: HashMap<i64, Vec<Sender<Command>>>,
    pub(crate) control_receiver: (Arc<Sender<Command>>, channel::Receiver<Command>),
    pub(crate) status: Status,
    pub transforms: HashMap<String, transform::Transform>,
}

#[cfg(test)]
impl Plan {
    pub(crate) fn get_result(&self, id: i64) -> Arc<Mutex<Vec<Train>>> {
        self.destinations.get(&id).unwrap().get_result_handle()
    }
}

impl Default for Plan {
    fn default() -> Self {
        let (tx, rx) = unbounded();
        Plan {
            id: GLOBAL_ID.new_id(),
            name: "".to_string(),
            lines: Default::default(),
            stations: Default::default(),
            stations_to_in_outs: Default::default(),
            sources: Default::default(),
            destinations: Default::default(),
            controls: Default::default(),
            control_receiver: (Arc::new(tx), rx),
            status: Stopped,
            transforms: Default::default(),
        }
    }
}

#[derive(Clone, Serialize)]
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
        let mut dump = self.dump_network();

        if !self.sources.is_empty() {
            dump += "\nIn\n";
            let mut sorted = self.sources.values().collect::<Vec<&Box<dyn Source>>>();
            sorted.sort_by_key(|s| s.get_name());
            dump += &sorted.into_iter().map(|s| s.dump_source()).collect::<Vec<_>>().join("\n")
        }

        if !self.destinations.is_empty() {
            dump += "\nOut\n";
            let mut sorted = self.destinations.values().collect::<Vec<&Box<dyn Destination>>>();
            sorted.sort_by_key(|s| s.get_name());
            dump += &sorted.into_iter().map(|s| s.dump_destination()).collect::<Vec<_>>().join("\n")
        }

        if !self.transforms.is_empty() {
            dump += "\nTransform\n";
            let mut sorted = self.transforms.values().collect::<Vec<&transform::Transform>>();
            sorted.sort_by_key(|s| s.get_name());
            dump += &sorted.into_iter().map(|s| s.dump()).collect::<Vec<_>>().join("\n")
        }

        dump
    }

    fn dump_network(&self) -> String {
        let mut dump = "".to_string();
        let mut dumped_stations = vec![];

        let mut lines: Vec<(&i64, &Vec<i64>)> = self.lines.iter().collect();
        lines.sort_by_key(|&(key, _)| key);
        for line in lines {
            if *line.0 != 0 {
                dump += "\n"
            }

            let mut last = -1;
            for (index, stop_number) in line.1.iter().enumerate() {
                if index != 0 {
                    dump += match &self.stations[stop_number] {
                        s if s.block.contains(&last) => "-|",
                        _ => "--",
                    };
                }
                dump += &self.stations[stop_number].dump(dumped_stations.contains(&stop_number));
                last = *stop_number;
                dumped_stations.push(stop_number)
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

        for station in self.stations.values_mut() {
            station.enrich(self.transforms.clone())
        }

        for station in self.stations.values_mut() {
            let entry = self.controls.entry(station.id).or_default();
            entry.push(station.operate(Arc::clone(&self.control_receiver.0)));
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


        for destination in self.destinations.values_mut() {
            self.controls.entry(destination.get_id()).or_default().push(destination.operate(Arc::clone(&self.control_receiver.0)));
        }

        for source in self.sources.values_mut() {
            self.controls.entry(source.get_id()).or_default().push(source.operate(Arc::clone(&self.control_receiver.0)));
        }


        self.status = Status::Running;
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


    pub fn parse(stencil: &str) -> Result<Self, String> {
        let mut plan = Plan::default();
        let mut phase = Stencil::Network;


        let lines = stencil.split('\n');
        for (line_number, line) in lines.enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            match line.to_lowercase().trim() {
                "in" => {
                    phase = Stencil::In;
                    continue;
                },
                "out" => {
                    phase = Stencil::Out;
                    continue;
                },
                "transform" => {
                    phase = Stencil::Transform;
                    continue;
                },
                _ => {}
            }

            match phase {
                Stencil::In => {
                    plan.parse_in(line)?;
                }
                Stencil::Out => {
                    plan.parse_out(line)?;
                }
                Stencil::Transform => {
                    plan.parse_transform(line)?;
                }
                _ => {
                    plan.parse_line(line_number as i64, line);
                }
            }
            
        }

        Ok(plan)
    }
    fn parse_line(&mut self, line: i64, stencil: &str) {
        let mut temp = String::default();
        let mut is_text = false;

        let mut last = None;

        let mut last_char = '_';

        for char in stencil.chars() {
            if is_text && char != '"' {
                temp.push(char);
                last_char = char;
                continue;
            }


            match char {
                // }--1 or }-|1
                '-' | '|' => {
                    if last_char == '-' {
                        let mut blueprint = temp.clone();
                        blueprint.remove(blueprint.len() - 1); // remove last }-

                        let station = Station::parse(blueprint, last);
                        last = Some(station.stop);
                        self.build(line, station);

                        temp = String::default();
                    }
                    temp.push(char);
                }

                '"' => {
                    is_text = !is_text;
                    temp.push(char);
                }
                _ => {
                    temp.push(char);
                }
            }
            last_char = char;
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
        for destination in self.destinations.values_mut() {
            let tx = destination.get_in();
            let target = destination.get_stop();
            if let Some(station) = self.stations.get_mut(&target) {
                station.add_out(-1, tx)?;
            } else {
                Err(String::from("Could not find target station"))?;
            }
        }
        Ok(())
    }
    fn connect_sources(&mut self) -> Result<(), String> {
        for source in self.sources.values_mut() {
            let target = source.get_stop();
            if let Some(station) = self.stations.get_mut(&target) {
                let tx = station.get_in();
                source.add_out(station.stop, tx)
            }
        }
        Ok(())
    }

    pub(crate) fn add_source(&mut self, stop: i64, source: Box<dyn Source>) {
        assert_eq!(stop, source.get_stop());
        let id = source.get_id();
        self.sources.insert(id, source);
        self.stations_to_in_outs.entry(id).or_default().push(stop);
    }

    pub(crate) fn add_destination(&mut self, stop: i64, destination: Box<dyn Destination>) {
        assert_eq!(stop, destination.get_stop());
        let id = destination.get_id();
        self.destinations.insert(id, destination);
        self.stations_to_in_outs.entry(id).or_default().push(stop);
    }

    fn parse_in(&mut self, stencil: &str) -> Result<(), String> {
        let (stop, type_, options) = Self::split_name_options(stencil)?;

        let source = parse_source(type_, options, stop)?;
        self.add_source(stop, source);
        Ok(())
    }

    fn split_name_options(stencil: &str) -> Result<(i64, &str, Map<String, Value>), String> {
        let (stencil, stop) = stencil.rsplit_once("}:").unwrap();
        let stop = stop.parse::<i64>().unwrap();

        let (type_, template) = stencil.split_once('{').ok_or(format!("Invalid template: {}", stencil))?;
        let json = format!("{{ {} }}", template.trim());
        let options = serde_json::from_str::<Value>(&json).unwrap().as_object().ok_or(format!("Invalid options: {}", template))?.clone();
        Ok((stop, type_, options))
    }

    fn parse_out(&mut self, stencil: &str) -> Result<(), String> {
        let (stop, type_, options) = Self::split_name_options(stencil)?;

        let out = parse_destination(type_, options, stop)?;
        self.add_destination(stop, out);
        Ok(())
    }

    fn parse_transform(&mut self, stencil: &str) -> Result<(), String> {
        let (name, stencil) = stencil.split_once(':').ok_or("No name for transformer provided")?;
        let transform = transform::Transform::parse(stencil)?;

        self.add_transform(name.trim_start_matches('$'), transform);
        Ok(())
    }

    fn add_transform(&mut self, name: &str, transform: transform::Transform) {
        self.transforms.insert(name.to_string(), transform);
    }
}

enum Stencil {
    Network,
    In,
    Out,
    Transform,
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
        state.serialize_field("status", &self.status)?;

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
            let ins_outs = self.stations_to_in_outs.get(&stop.stop).cloned().unwrap_or(vec![]);
            stops.insert(
                num.to_string(),
                Stop {
                    num: *num,
                    transform: stop.transform.clone().map(|t| {
                        t.into()
                    }),
                    sources: ins_outs.clone().iter()
                        .map(|s| self.sources.get(s))
                        .filter(|s| s.is_some())
                        .map(|s| s.unwrap().serialize())
                        .collect(),
                    destinations: ins_outs.clone().iter()
                        .map(|s| self.destinations.get(s))
                        .filter(|s| s.is_some())
                        .map(|s| s.unwrap().serialize())
                        .collect(),
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


impl From<transform::Transform> for ConfigContainer {
    fn from(value: transform::Transform) -> Self {
        match value {
            transform::Transform::Func(_) => {
                let mut map = HashMap::new();
                map.insert(String::from("type"), ConfigModel::String(StringModel::new("Function")));
                map.insert(String::from("query"), ConfigModel::String(StringModel::new("")));
                ConfigContainer::new(String::from("Transform"), map)
            }
            transform::Transform::Lang(l) => {
                let mut map = HashMap::new();
                map.insert(String::from("language"), ConfigModel::String(StringModel::new(&l.language.to_string())));
                map.insert(String::from("query"), ConfigModel::String(StringModel::new(&l.query.to_string())));
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
            "1--2",
            "1--2--3",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn parse_line_different_modes() {
        let stencils = vec![
            "1",
            "1--2",
            "1-|2"
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn parse_multiline_stop_stencil() {
        let stencils = vec![
            "1--2\n\
            3--2",
            "1--2--3\n\
            4--3",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn stencil_transform_sql() {
        let stencils = vec![
            "1--2{sql|SELECT * FROM $1}",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump(), stencil)
        }
    }

    //#[test]
    fn stencil_transform_mql() {
        let stencils = vec![
            "1--2{sql|db.$1.find({})}",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump(), stencil)
        }
    }


    #[test]
    fn stencil_window() {
        let stencils = vec![
            "1--2[3s]",
            "1--2[3s@13:15]",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn stencil_block() {
        let stencils = vec![
            "1--2--3\n\
            4-|2",
            "1-|2--3\n\
            4--2",
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn stencil_branch() {
        let stencils = vec![
            "1--2{sql|SELECT $1.name FROM $1}\n\
            1--3{sql|SELECT $1.age FROM $1}",
            /*"1-2{sql|$1 HAS name}\n1-3{sql|SELECT $1.age FROM $1}",
            "1-2{sql|$1 HAS NOT name}\n1-3{sql|SELECT $1.age FROM $1}",
            //
            "1-2{mql|db.$1.has(name: 1)}\n1-3{db.$1.find({},{age:1}}",*/
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn stencil_source() {
        let stencils = vec![
            "\
            1--2\n\
            In\n\
            Mqtt{\"port\":8080,\"url\":\"127.0.0.1\"}:1\
            "
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn stencil_destination() {
        let stencils = vec![
            "\
            1--2\n\
            In\n\
            Mqtt{\"port\":8080,\"url\":\"127.0.0.1\"}:1\n\
            Out\n\
            Dummy{\"result_size\":0}:1\
            "
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump(), stencil)
        }
    }

    #[test]
    fn stencil_source_and_destination() {
        let stencils = vec![
            "\
            1--2\n\
            Out\n\
            Dummy{\"result_size\":0}:1\
            "
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump(), stencil)
        }
    }
}

