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
        self.plan.stations.values().for_each(|s| {
            if !self.plan.lines.iter().any(|(_num, l)| l.contains(&s.id)) && !self.plan.stations_to_in_outs.contains_key(&s.id){
                todo!();
                summery.add_stop_status(s.id, Status::WARNING(Islands, format!("Station {} is not connected to anything.", s.id)));
            }
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
        let plan = Plan::parse("In\nDummy{}").unwrap();
        let analyse = analyse(&plan);
        assert!(analyse.is_ok());
        let analyse = analyse.unwrap();
        assert!(!analyse.is_ok());
        let warnings = analyse.get_warnings();

        assert_eq!(warnings.len(), 1);
        match warnings[0].clone() {
            Status::WARNING(st, _) => {
                assert_eq!(StatusTypes::Islands, st);
            }
            _ => panic!("Wrong type of status")
        }
    }
}