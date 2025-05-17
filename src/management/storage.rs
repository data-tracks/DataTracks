use crate::processing::destination::Destination;
use crate::processing::source::Source;
use crate::processing::{transform, Plan};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Mutex;
use schemas::message_generated::protocol::Create;
use crate::processing::plan::Status;

#[derive(Default)]
pub struct Storage {
    pub plans: Mutex<HashMap<usize, Plan>>,
    pub ins: Mutex<HashMap<String, fn(Map<String, Value>) -> Box<dyn Source>>>,
    pub outs: Mutex<HashMap<String, fn(Map<String, Value>) -> Box<dyn Destination>>>,
    pub transforms: Mutex<HashMap<String, fn(String, Value) -> Box<transform::Transform>>>
}


impl Storage {
    pub(crate) fn new() -> Storage {
        Default::default()
    }

    pub fn create_plan(&mut self, create: Create) -> Result<(), String> {
        let create_plan = create.create_type_as_create_plan().unwrap();
        if create_plan.name().is_some() && create_plan.plan().is_some() {
            let plan = Plan::parse(create_plan.plan().unwrap());

            let mut plan = match plan {
                Ok(plan) => plan,
                Err(e) => todo!(),
            };

            plan.set_name(create_plan.name().unwrap().to_string());
            self.add_plan(plan);
            Ok(())
        }else {
            Err("No name provided with create plan".to_string())
        }
    }

    pub fn add_plan(&mut self, plan: Plan) {
        let mut plans = self.plans.lock().unwrap();
        plans.insert(plan.id, plan);
    }

    pub fn add_source(&mut self, plan_id: usize, stop_id: usize, source: Box<dyn Source>) {
        let mut plans = self.plans.lock().unwrap();
        let id = source.id();
        if let Some(p) = plans.get_mut(&plan_id) {
            p.add_source(source);
            p.connect_in_out(stop_id, id);
        }
    }

    pub fn add_destination(&mut self, plan_id: usize, stop_id: usize, destination: Box<dyn Destination>) {
        let mut plans = self.plans.lock().unwrap();
        let id = destination.get_id();
        if let Some(p) = plans.get_mut(&plan_id) {
            p.add_destination(destination);
            p.connect_in_out(stop_id, id);
        }
    }

    pub fn start_plan_by_name(&mut self, name: String) {
        let mut lock = self.plans.lock().unwrap();
        let plan = lock.iter_mut().filter(|(_id, plan)| plan.name == name).map(|(_,plan)| plan).next();
        match plan {
            None => {}
            Some(p) => {
                p.operate().unwrap();
            }
        }
    }

    pub fn start_plan(&mut self, id: usize) {
        let mut lock = self.plans.lock().unwrap();
        let plan = lock.get_mut(&id);
        match plan {
            None => {}
            Some(p) => {
                p.status = Status::Running;
                p.operate().unwrap();
            }
        }
    }
}