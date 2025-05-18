use crate::processing::destination::Destination;
use crate::processing::source::Source;
use crate::processing::{transform, Plan, Train};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use axum::Router;
use axum::routing::get;
use crossbeam::channel::at;
use schemas::message_generated::protocol::{Create};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::error;
use crate::http::destination::{DestinationState};
use crate::http::util;
use crate::processing::plan::Status;
use crate::processing::station::Command;
use crate::util::Tx;
use crate::util::new_channel;

#[derive(Default)]
pub struct Storage {
    pub plans: Mutex<HashMap<usize, Plan>>,
    pub ins: Mutex<HashMap<String, fn(Map<String, Value>) -> Box<dyn Source>>>,
    pub outs: Mutex<HashMap<String, fn(Map<String, Value>) -> Box<dyn Destination>>>,
    pub transforms: Mutex<HashMap<String, fn(String, Value) -> Box<transform::Transform>>>,
    pub attachments: Mutex<HashMap<usize, Tx<Train>>>
}


impl Storage {
    pub(crate) fn new() -> Storage {
        Default::default()
    }
    
    pub fn start_http_attacher(&mut self, id: usize, port: u16) -> Tx<Train> {
        let addr = util::parse_addr("http://127.0.0.1", port);
        let (tx,_, rx) = new_channel();

        tokio::spawn(async move {
            let state = DestinationState {
                rx: Arc::new(Mutex::new(rx)),
            };

            let app = Router::new()
                .route("/ws", get(util::publish_ws))
                .layer(CorsLayer::permissive())
                .with_state(state);

            let listener = TcpListener::bind(&addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });
        self.attachments.lock().unwrap().insert(id, tx.clone());
        tx
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

    pub fn attach(&mut self, source_id:usize, plan_id: usize, stop_id: usize) -> Result<usize, String> {
        let attach = self.attachments.lock().unwrap();
        if attach.contains_key(&source_id) {
           return Err(format!("source id already exists: {}", source_id)); 
        }
        drop(attach);
        
        let tx = self.start_http_attacher(source_id, 3131);
        let mut lock = self.plans.lock().unwrap();
        let plan = lock.get_mut(&plan_id).unwrap();
        match plan.stations.get_mut(&stop_id) {
            None => error!("Could not find plan"),
            Some(station) => {
                station.control.0.send(Command::Attach(source_id, tx)).unwrap();
            }
        }
        Ok(3131)
    }

    pub fn detach(&mut self, source_id: usize, plan_id: usize, stop_id: usize) {
        let attach = self.attachments.lock().unwrap();
        if !attach.contains_key(&source_id) {
            return;
        }
        drop(attach);
        
        let mut lock = self.plans.lock().unwrap();
        let plan = lock.get_mut(&plan_id).unwrap();
        match plan.stations.get_mut(&stop_id) {
            None => error!("Could not find plan"),
            Some(station) => {
                station.control.0.send(Command::Detach(source_id)).unwrap();
            }
        }
        let mut lock = self.attachments.lock().unwrap();
        match lock.remove(&source_id) {
            None => {}
            Some(_) => {}
        };
    }
}