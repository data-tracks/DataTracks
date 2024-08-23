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

    pub fn start_plan(&mut self, id: i64) {
        let mut lock = self.plans.lock().unwrap();
        let plan = lock.get_mut(&id);
        match plan {
            None => {}
            Some(mut p) => {
                p.operate();
            }
        }
    }
}