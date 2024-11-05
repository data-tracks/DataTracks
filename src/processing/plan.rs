use crate::processing::destination::{parse_destination, Destination};
use crate::processing::option::Configurable;
use crate::processing::plan::Status::Stopped;
use crate::processing::source::{parse_source, Source};
use crate::processing::station::{Command, Station};
use crate::processing::{transform, Train};
use crate::ui::{ConfigContainer, ConfigModel};
use crate::util::GLOBAL_ID;
use core::default::Default;
use crossbeam::channel::{unbounded, Receiver, Sender};
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
    pub stations: HashMap<i64, Station>,
    pub stations_to_in_outs: HashMap<i64, Vec<i64>>,
    pub sources: HashMap<i64, Box<dyn Source>>,
    pub destinations: HashMap<i64, Box<dyn Destination>>,
    pub controls: HashMap<i64, Vec<Sender<Command>>>,
    pub control_receiver: (Arc<Sender<Command>>, Receiver<Command>),
    pub status: Status,
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
            dump += &sorted.into_iter().map(|s| {
                let mut stops = self.get_connected_stations(s.get_id());
                stops.sort();
                if stops.is_empty() {
                    s.dump_source().to_string()
                }else {
                    format!("{}:{}", s.dump_source(), stops.iter().map(|s|s.to_string()).collect::<Vec<String>>().join(",") )
                }

            }).collect::<Vec<_>>().join("\n")
        }

        if !self.destinations.is_empty() {
            dump += "\nOut\n";
            let mut sorted = self.destinations.values().collect::<Vec<&Box<dyn Destination>>>();
            sorted.sort_by_key(|s| s.get_name());
            dump += &sorted.into_iter().map(|s| {

                let stops = self.get_connected_stations(s.get_id());
                if stops.is_empty() {
                    s.dump_destination().to_string()
                }else {
                    format!("{}:{}", s.dump_destination(), stops.iter().map(|s|s.to_string()).collect::<Vec<String>>().join(",") )
                }
            }).collect::<Vec<_>>().join("\n")
        }

        if !self.transforms.is_empty() {
            dump += "\nTransform\n";
            let mut sorted = self.transforms.keys().collect::<Vec<_>>();
            sorted.sort();
            dump += &sorted.into_iter().map(|name| {
                let dump = self.transforms.get(name).unwrap().dump();
                format!("${}:{}", name, dump)
            }).collect::<Vec<_>>().join("\n")
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

    pub fn operate(&mut self) -> Result<(), String> {
        self.connect_stops()?;
        self.connect_destinations()?;
        self.connect_sources()?;

        for station in self.stations.values_mut() {
            let entry = self.controls.entry(station.id).or_default();
            entry.push(station.operate(Arc::clone(&self.control_receiver.0), self.transforms.clone()));
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
                        command => return Err(format!("Not known command {:?}", command)),
                    }
                }
                Err(err) => return Err(err.to_string()),
            }
        }


        for destination in self.destinations.values_mut() {
            self.controls.entry(destination.get_id()).or_default().push(destination.operate(Arc::clone(&self.control_receiver.0)));
        }

        for source in self.sources.values_mut() {
            self.controls.entry(source.get_id()).or_default().push(source.operate(Arc::clone(&self.control_receiver.0)));
        }


        self.status = Status::Running;
        Ok(())
    }

    pub(crate) fn clone_platform(&mut self, num: i64) {
        let station = self.stations.get_mut(&num).unwrap();
        self.controls.entry(num).or_default().push(station.operate(Arc::clone(&self.control_receiver.0), self.transforms.clone()))
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
                    let _ = plan.parse_line(line_number as i64, line);
                }
            }
            
        }

        Ok(plan)
    }
    fn parse_line(&mut self, line: i64, stencil: &str) -> Result<(), String> {
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

                        let station = Station::parse(blueprint, last)?;
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
            let station = Station::parse(temp, last)?;
            self.build(line, station);
        }
        Ok(())
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
        let mut map = HashMap::new();
        self.destinations.iter().for_each(|destination| {
            map.insert(destination.1.get_id(), self.get_connected_stations(destination.1.get_id()));
        });

        for destination in self.destinations.values_mut() {
            let targets = map.get(&destination.get_id()).unwrap();
            for target in targets {
                if let Some(station) = self.stations.get_mut(target) {
                    station.add_out(-destination.get_id(), destination.get_in())?; // maybe change negative approach
                } else {
                    Err(String::from("Could not find target station"))?;
                }
            }

        }
        Ok(())
    }
    fn connect_sources(&mut self) -> Result<(), String> {
        let mut map = HashMap::new();
        self.sources.iter().for_each(|source| {
            map.insert(source.1.get_id(), self.get_connected_stations(source.1.get_id()));
        });

        for source in self.sources.values_mut() {
            let targets = map.get(&source.get_id()).unwrap();
            for target in targets {
                if let Some(station) = self.stations.get_mut(target) {
                    let tx = station.get_in();
                    source.add_out(tx)
                }
            }

        }
        Ok(())
    }

    fn get_connected_stations(&self, in_out: i64) -> Vec<i64> {
        let targets = self.stations_to_in_outs.iter().flat_map(|(stop, in_outs)| {
            if in_outs.contains(&in_out) {
                vec![*stop]
            } else {
                vec![]
            }
        }).collect::<Vec<i64>>();
        targets
    }

    pub fn connect_in_out(&mut self, stop: i64, in_out: i64) {
        self.stations_to_in_outs.entry(stop).or_default().push(in_out);
    }


    pub(crate) fn add_source(&mut self, source: Box<dyn Source>) {
        let id = source.get_id();
        self.sources.insert(id, source);
    }

    pub(crate) fn add_destination(&mut self, destination: Box<dyn Destination>) {
        let id = destination.get_id();
        self.destinations.insert(id, destination);
    }

    fn parse_in(&mut self, stencil: &str) -> Result<(), String> {
        let (stops, type_, options) = Self::split_name_options(stencil)?;

        let source = parse_source(type_, options)?;
        let id = source.get_id();
        self.add_source(source);
        for stop in stops {
            self.connect_in_out(stop, id);
        }

        Ok(())
    }

    fn split_name_options(stencil: &str) -> Result<(Vec<i64>, &str, Map<String, Value>), String> {
        let (stencil, stops) = stencil.rsplit_once("}:").unwrap();
        let stops = stops.split(',').map(|num| num.parse::<i64>()).collect::<Result<Vec<_>, _>>().map_err(|err| err.to_string())?;

        let (type_name, template) = stencil.split_once('{').ok_or(format!("Invalid template: {}", stencil))?;
        let json = format!("{{ {} }}", template.trim());
        let options = serde_json::from_str::<Value>(&json)
            .map_err(|e| format!("Could not parse options for {}: {}", type_name, e.to_string()))?
            .as_object().ok_or(format!("Invalid options: {}", template))?.clone();
        Ok((stops, type_name, options))
    }

    fn parse_out(&mut self, stencil: &str) -> Result<(), String> {
        let (stops, type_, options) = Self::split_name_options(stencil)?;

        let out = parse_destination(type_, options)?;
        let id = out.get_id();
        self.add_destination(out);
        stops.iter().for_each(|s| self.connect_in_out(*s, id));

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

    fn get_station(&self, stop_num: &i64) -> Result<&Station, String> {
        self.stations.get(stop_num).ok_or_else(|| format!("Station {} not found", stop_num))
    }

    pub fn layouts_match(&self) -> Result<(), String> {
        let mut layouts = HashMap::new(); // track all known station outputs

        while self.stations.len() != layouts.len() { // we should collect all layouts in the end
            for (line, stops) in &self.lines {
                if stops.is_empty() {
                    continue;
                }
                let mut iter = stops.iter();
                let station_num = *iter.next().unwrap();
                let mut layout = self.get_station(&station_num)?.derive_output_layout(HashMap::new());
                layouts.insert(station_num, layout.clone());

                for stop_num in stops {
                    if layouts.contains_key(stop_num) {
                        continue;
                    }
                    let station = self.get_station(stop_num)?;

                    let stations = self.get_previous_stations(stop_num);
                    if !stations.keys().all(|num| layouts.contains_key(num)) {
                        // let's try later
                        continue
                    }
                    let mut inputs = HashMap::new();
                    stations.into_iter().for_each(|(num, _station)| {
                        inputs.insert(num.to_string(), layouts.get(&num).unwrap());
                    });

                    if !station.derive_input_layout().accepts(&layout) {
                        return Err(format!("On line {} station {} does not accept the previous input", line, stop_num));
                    }
                    let current = station.derive_output_layout(inputs);

                    layouts.insert(*stop_num, current.clone());
                    layout = current;
                }
            }
        }

        Ok(())
    }

    fn get_previous_stations(&self, station: &i64) -> HashMap<i64, &Station> {
        let mut befores = HashMap::new();
        for (_, stations) in &self.lines {
            if stations.is_empty() {
                continue;
            }
            let mut iter = stations.iter();
            let mut previous = iter.next().unwrap();
            for station_num in iter {
                if station_num == station {
                    befores.insert(*previous, self.get_station(previous).unwrap());
                }
                previous = station_num;
            }
        }
        befores
    }
}

pub fn check_commands(rx: &Receiver<Command>) -> bool {
    if let Ok(command) = rx.try_recv() {
        match command {
            Command::Stop(_) => return true,
            _ => {}
        }
    }
    false
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
                map.insert(String::from("type"), "Function".into());
                map.insert(String::from("query"), "".into());
                ConfigContainer::new(String::from("Transform"), map)
            }
            transform::Transform::Lang(l) => {
                let mut map = HashMap::new();
                map.insert(String::from("language"), l.language.name().into());
                map.insert(String::from("query"), l.query.into());
                ConfigContainer::new(String::from("Transform"), map)
            }

            transform::Transform::SQLite(l) => {
                let mut map = HashMap::new();
                map.insert(String::from("query"), l.query.get_query().into());
                map.insert(String::from("path"), l.connector.path.clone().into());
                ConfigContainer::new(l.get_name(), map)
            }
            transform::Transform::Postgres(p) => {
                let mut map = HashMap::new();
                map.insert(String::from("query"), p.query.get_query().into());
                map.insert(String::from("url"), p.connector.url.clone().into());
                map.insert(String::from("port"), p.connector.port.into());
                map.insert(String::from("db"), p.connector.db.clone().into());
                ConfigContainer::new(p.get_name(), map)
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

    #[test]
    fn stencil_multiple_sources() {
        let stencils = vec![
            "\
            1--2\n\
            In\n\
            Mqtt{\"port\":8080,\"url\":\"127.0.0.1\"}:1,2\n\
            "
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();
            assert_eq!(plan.dump().trim(), stencil.trim())
        }
    }
}

