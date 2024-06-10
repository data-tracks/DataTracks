use std::collections::HashMap;

use crate::processing::destination::Destination;
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
        let mut  dump = "".to_string();
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
}