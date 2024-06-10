use std::collections::HashMap;

use crate::processing::destination::Destination;
use crate::processing::plan::PlanStage::{Block, Num, Transform, Window};
use crate::processing::source::Source;
use crate::processing::station::Station;
use crate::util::GLOBAL_ID;

struct Plan {
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

    fn dump(&self) -> String {
        let mut dump = "".to_string();
        let mut lines: Vec<(&i64, &Vec<i64>)> = self.lines.iter().collect();
        lines.sort_by_key(|&(key, _)| key);
        for line in lines {
            for stop in line.1 {
                dump += &*self.stations[stop].dump()
            }
            dump += "\n"
        }

        dump
    }

    pub fn parse(stencil: String) -> Self {
        let mut plan = Plan::default();

        let lines = stencil.split("\n");
        for line in lines {
            plan.parse_line(line);
        }

        plan
    }
    fn parse_line(&mut self, stencil: &str) {
        let mut temp = "".to_string();
        let mut stage = Num;
        let mut current: Vec<(PlanStage, String)> = vec![];
        let mut is_text = false;

        for char in stencil.chars() {
            match char {
                '-' => {
                    if is_text {
                        temp.push(char);
                        return;
                    }

                    match stage {
                        Num => current.push((stage, temp.clone())),
                        _ => {}
                    };

                    self.parse_stop(&current);
                    current.clear();
                    stage = Num;
                }
                '{' | '(' | '[' => {
                    match stage {
                        Num => current.push((stage, temp.clone())),
                        _ => {}
                    };
                    match char {
                        '[' => stage = Block,
                        '(' => stage = Window,
                        '{' => stage = Transform,
                        _ => {}
                    }
                    stage = Transform;
                }
                '}' | ')' | ']' => {
                    current.push((stage, temp.clone()));
                    temp = "".to_string();
                }
                '"' => {
                    is_text = !is_text;
                    temp.push(char);
                }
                _ => temp.push(char),
            }
        }
    }

    fn parse_stop<'a>(&mut self, parts: &'a Vec<(PlanStage, String)>) {
        for stage in parts {
            match (*stage).0 {
                Window => {}
                Transform => {}
                Block => {}
                Num => {}
            }
        }
    }
}

struct StartEnd(char, char);

#[derive(Clone, Copy)]
enum PlanStage {
    Window,
    Transform,
    Block,
    Num,
}