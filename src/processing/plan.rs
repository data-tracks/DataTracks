use std::collections::HashMap;

use crate::processing::destination::Destination;
use crate::processing::plan::PlanStage::{BlockStage, Num, TransformStage, WindowStage};
use crate::processing::source::Source;
use crate::processing::station::Station;
use crate::processing::transform::Transform;
use crate::processing::window::Window;
use crate::util::GLOBAL_ID;

pub(crate) struct Plan {
    id: i64,
    lines: HashMap<i64, Vec<i64>>,
    stations: HashMap<i64, Station>,
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

    pub(crate) fn dump(&self) -> String {
        let mut dump = "".to_string();
        let mut lines: Vec<(&i64, &Vec<i64>)> = self.lines.iter().collect();
        lines.sort_by_key(|&(key, _)| key);
        for line in lines {
            if *line.0 != 0 {
                dump += "\n"
            }

            for stop in line.1.iter().enumerate() {
                if stop.0 != 0 {
                    dump += "-";
                }
                dump += &self.stations[stop.1].dump(line.0)
            }
        }

        dump
    }

    pub fn parse(stencil: &str) -> Self {
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

                    let station = self.parse_stop(line, current.clone());
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
            let station = self.parse_stop(line, current.clone());
            self.build(line, station);
        }
    }

    fn parse_stop(&mut self, line: i64, parts: Vec<(PlanStage, String)>) -> Station {
        let mut station: Station = Station::default();
        for stage in parts {
            match stage.0 {
                WindowStage => station.window(Window::parse(stage.1)),
                TransformStage => station.transform(Transform::parse(stage.1).unwrap()),
                BlockStage => station.block(line),
                Num => station.stop(stage.1.parse::<i64>().unwrap()),
            }
        }
        station
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
}

struct StartEnd(char, char);

#[derive(Clone, Copy)]
enum PlanStage {
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
}