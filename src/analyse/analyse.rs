use crate::analyse::summery::StatusTypes::Islands;
use crate::analyse::summery::{Status, Summery};
use crate::processing::Plan;

pub struct Analyser<'plan>{
    pub plan: &'plan Plan,
}


impl<'plan> Analyser<'plan> {
    pub fn new(plan: &'plan Plan) -> Self {
        Analyser { plan }
    }

    fn analyse(&self) -> Result<Summery, String> {
        let mut summery = Summery::new();

        // analyse layouts
        // cyclic
        // islands?
        //stations
        self.plan.lines.values().for_each(|s| {
            if s.is_empty() {
                return
            }
            let start = s.clone().remove(0);
            // 1--2 -> 1 & 2
            self.check_in_and_out(&mut summery, start.clone(), String::from(format!("Station {} is not connected to an in or output.", start.clone())));
            let end = s.clone().pop().unwrap();
            if end == start {
                return
            }
            self.check_in_and_out(&mut summery, end.clone(), String::from(format!("Station {} is not connected to an in or output.", end)));
        });

        self.plan.sources.values().for_each(|s| {
            if !self.plan.stations_to_in_outs.iter().any(|(_, in_outs)|{ in_outs.contains(&s.get_id()) }){
                summery.add_in_status(s.get_id(), Status::WARNING(Islands, format!("Source {} is not connected to anything.", s.get_id())));
            }
        });

        self.plan.destinations.values().for_each(|s| {
            if !self.plan.stations_to_in_outs.iter().any(|(_, in_outs)|{ in_outs.contains(&s.get_id()) }){
                summery.add_out_status(s.get_id(), Status::WARNING(Islands, format!("Destination {} is not connected to anything.", s.get_id())));
            }
        });

        Ok(summery)
    }

    fn check_in_and_out(&self, summery: &mut Summery, station: i64, error: String) {
        let between_stops = self.plan.lines.values().cloned().flat_map(|mut stops| {
            if stops.is_empty() {
                vec![]
            }else {
                stops.pop();
                if stops.is_empty() {
                    vec![]
                }else{
                    stops.remove(0);
                    stops
                }
            }
        }).collect::<Vec<i64>>();

        if (!self.plan.stations_to_in_outs.contains_key(&station) || self.plan.stations_to_in_outs.get(&station).unwrap().is_empty()) && !between_stops.contains(&station) {
            summery.add_stop_status(station, Status::WARNING(Islands, error));
        }
    }
}

pub fn analyse(plan: &Plan) -> Result<Summery, String> {
    let analyse = Analyser::new(plan);
    analyse.analyse()
}

#[cfg(test)]
mod tests {
    use crate::analyse::analyse::analyse;
    use crate::analyse::summery::{Status, StatusTypes};
    use crate::processing::Plan;

    #[test]
    fn test_islands(){
        let plan = Plan::parse("1").unwrap();
        let analyse = analyse(&plan);
        let analyse = analyse.unwrap();
        assert!(!analyse.is_ok());
        let warnings = analyse.get_warnings();

        assert_eq!(warnings.len(), 1);
        match warnings[0].clone() {
            Status::WARNING(st, _) => {
                assert_eq!(StatusTypes::Islands, st);
            }
            s => panic!("Wrong type of status: {:?}", s)
        }
    }
}