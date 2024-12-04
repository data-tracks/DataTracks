use crate::processing::destination::Destination;
use crate::processing::source::Source;
use crate::processing::{transform, Plan};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct Storage {
    pub plans: Mutex<HashMap<i64, Plan>>,
    pub ins: Mutex<HashMap<String, fn(Map<String, Value>) -> Box<dyn Source>>>,
    pub outs: Mutex<HashMap<String, fn(Map<String, Value>) -> Box<dyn Destination>>>,
    pub transforms: Mutex<HashMap<String, fn(String, Value) -> Box<transform::Transform>>>
}


impl Storage {
    pub(crate) fn new() -> Storage {
        Default::default()
    }

    pub fn add_plan(&mut self, plan: Plan) {
        let mut plans = self.plans.lock().unwrap();
        plans.insert(plan.id, plan);
    }

    pub fn add_source(&mut self, plan_id: i64, stop_id: i64, source: Box<dyn Source>) {
        let mut plans = self.plans.lock().unwrap();
        let id = source.get_id();
        if let Some(p) = plans.get_mut(&plan_id) {
            p.add_source(source);
            p.connect_in_out(stop_id, id);
        }
    }

    pub fn add_destination(&mut self, plan_id: i64, stop_id: i64, destination: Box<dyn Destination>) {
        let mut plans = self.plans.lock().unwrap();
        let id = destination.get_id();
        if let Some(p) = plans.get_mut(&plan_id) {
            p.add_destination(destination);
            p.connect_in_out(stop_id, id);
        }
    }

    pub fn start_plan_by_name(&mut self, name: String) {
        let mut lock = self.plans.lock().unwrap();
        let plan = lock.iter_mut().filter(|(id, plan)| plan.name == name).map(|(_,plan)| plan).next();
        match plan {
            None => {}
            Some(p) => {
                p.operate().unwrap();
            }
        }
    }

    pub fn start_plan(&mut self, id: i64) {
        let mut lock = self.plans.lock().unwrap();
        let plan = lock.get_mut(&id);
        match plan {
            None => {}
            Some(p) => {
                p.operate().unwrap();
            }
        }
    }
}