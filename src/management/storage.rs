use crate::processing::destination::Destination;
use crate::processing::source::Source;
use crate::processing::Plan;
use std::collections::HashMap;
use std::sync::Mutex;

pub struct Storage {
    pub plans: Mutex<HashMap<i64, Plan>>,
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}


impl Storage {
    pub(crate) fn new() -> Storage {
        Storage { plans: Mutex::new(HashMap::new()) }
    }

    pub fn add_plan(&mut self, plan: Plan) {
        let mut plans = self.plans.lock().unwrap();
        plans.insert(plan.id, plan);
    }

    pub fn add_source(&mut self, plan_id: i64, stop_id: i64, source: Box<dyn Source>) {
        let mut plans = self.plans.lock().unwrap();
        if let Some(p) = plans.get_mut(&plan_id) {
            p.add_source(stop_id, source)
        }
    }

    pub fn add_destination(&mut self, plan_id: i64, stop_id: i64, destination: Box<dyn Destination>) {
        let mut plans = self.plans.lock().unwrap();

        if let Some(p) = plans.get_mut(&plan_id) {
            p.add_destination(stop_id, destination)
        }
    }

    pub fn start_plan(&mut self, id: i64) {
        let mut lock = self.plans.lock().unwrap();
        let plan = lock.get_mut(&id);
        match plan {
            None => {}
            Some(p) => {
                p.operate();
            }
        }
    }
}