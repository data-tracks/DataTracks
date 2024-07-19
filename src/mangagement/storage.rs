use std::collections::HashMap;
use std::sync::Mutex;

use crate::processing::Plan;

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
}