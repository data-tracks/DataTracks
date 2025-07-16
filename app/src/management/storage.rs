use crate::http::util;
use crate::http::util::{parse_addr, DestinationState};
use crate::processing::destination::{Destination, Destinations};
use crate::processing::plan::Status;
use crate::processing::source::{Source, Sources};
use crate::processing::station::Command;
use crate::processing::{transform, Plan, Train};
use crate::util::new_channel;
use crate::util::Tx;
use axum::routing::get;
use axum::Router;
use track_rails::message_generated::protocol::{CreatePlanRequest};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tower_http::cors::CorsLayer;
use tracing::error;
use value::Time;

#[derive(Default)]
pub struct Storage {
    pub plans: Mutex<HashMap<usize, Plan>>,
    pub ins: Mutex<HashMap<String, fn(Map<String, Value>) -> Box<dyn Source>>>,
    pub outs: Mutex<HashMap<String, fn(Map<String, Value>) -> Box<dyn Destination>>>,
    pub transforms: Mutex<HashMap<String, fn(String, Value) -> Box<transform::Transform>>>,
    pub attachments: Mutex<HashMap<usize, Attachment>>,
}

pub struct Attachment {
    data_port: u16,
    watermark_port: u16,
    sender: Tx<Train>,
    wm_sender: Tx<Time>,
    shutdown_channel: Sender<bool>,
    wm_shutdown_channel: Sender<bool>,
}

impl Attachment {
    pub fn new(
        data_port: u16,
        watermark_port: u16,
        sender: Tx<Train>,
        wm_sender: Tx<Time>,
        shutdown_channel: Sender<bool>,
        wm_shutdown_channel: Sender<bool>,
    ) -> Self {
        Attachment {
            watermark_port,
            wm_sender,
            data_port,
            sender,
            wm_shutdown_channel,
            shutdown_channel,
        }
    }
}

impl Storage {
    pub(crate) fn new() -> Storage {
        Default::default()
    }

    pub fn start_http_attacher(
        &mut self,
        id: usize,
        data_port: u16,
        watermark_port: u16,
    ) -> (Tx<Train>, Tx<Time>) {
        let addr = match parse_addr("127.0.0.1", data_port) {
            Ok(addr) => addr,
            Err(err) => panic!("{}", err),
        };

        let (tx, _) = new_channel::<Train, &str>("HTTP Attacher", true);

        let (sh_tx, sh_rx) = oneshot::channel();
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            let state = DestinationState::train("HTTP Attacher", tx_clone);
            let app = Router::new()
                .route("/ws", get(util::publish_ws))
                .layer(CorsLayer::permissive())
                .with_state(state);

            let listener = match TcpListener::bind(&addr).await {
                Ok(listener) => listener,
                Err(err) => panic!("Error attach {err}"),
            };
            match axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    sh_rx.await.ok();
                })
                .await
            {
                Ok(_) => {}
                Err(err) => error!("{}", err),
            }
        });

        // watermark
        let addr = match parse_addr("127.0.0.1", watermark_port) {
            Ok(addr) => addr,
            Err(err) => panic!("{}", err),
        };

        let (water_sh_tx, water_sh_rx) = oneshot::channel();
        let (water_tx, _) = new_channel::<Time, &str>("HTTP Attacher Watermark", true);

        let water_tx_clone = water_tx.clone();
        tokio::spawn(async move {
            let state = DestinationState::time("HTTP Watermark Attacher", water_tx_clone);
            let app = Router::new()
                .route("/ws", get(util::publish_ws))
                .layer(CorsLayer::permissive())
                .with_state(state);

            let listener = match TcpListener::bind(&addr).await {
                Ok(listener) => listener,
                Err(err) => panic!("Error attach {err}"),
            };
            match axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    water_sh_rx.await.ok();
                })
                .await
            {
                Ok(_) => {}
                Err(err) => error!("{}", err),
            }
        });

        let attachment = Attachment::new(
            data_port,
            watermark_port,
            tx.clone(),
            water_tx.clone(),
            sh_tx,
            water_sh_tx,
        );

        self.attachments.lock().unwrap().insert(id, attachment);
        (tx, water_tx)
    }

    pub fn create_plan(&mut self, create_plan: CreatePlanRequest) -> Result<usize, String> {
        if create_plan.name().is_some() && create_plan.plan().is_some() {
            let plan = Plan::parse(create_plan.plan().unwrap());

            let mut plan = match plan {
                Ok(plan) => plan,
                Err(_) => todo!(),
            };

            let id = plan.id;

            plan.set_name(create_plan.name().unwrap().to_string());
            self.add_plan(plan);
            Ok(id)
        } else {
            Err("No name provided with create plan".to_string())
        }
    }

    pub fn delete_plan(&mut self, id: usize) -> Result<(), String> {
        let mut plans = self.plans.lock().unwrap();
        plans.remove(&id).ok_or(format!("No plan with id {}", id)).map(|_| ())
    }

    pub fn get_plans_by_name<S: AsRef<str>>(&self, name: S) -> Vec<Plan> {
        if name.as_ref().trim().is_empty() || name.as_ref().trim() == "*" {
            return self.plans.lock().unwrap().clone().iter().map(|(_, plan)| plan.clone()).collect();
        }

        self.plans.lock().unwrap().iter()
            .filter(|(_, p)|{
                p.name.matches(name.as_ref()).next().is_some()
            })
            .map(|(_,p)|p.clone())
            .collect()
    }

    pub fn add_plan(&mut self, plan: Plan) {
        let mut plans = self.plans.lock().unwrap();
        plans.insert(plan.id, plan);
    }

    pub fn add_source(&mut self, plan_id: usize, stop_id: usize, source: Sources) {
        let mut plans = self.plans.lock().unwrap();
        let id = source.id();
        if let Some(p) = plans.get_mut(&plan_id) {
            p.add_source(source);
            p.connect_in_out(stop_id, id);
        }
    }

    pub fn add_destination(
        &mut self,
        plan_id: usize,
        stop_id: usize,
        destination: Destinations,
    ) {
        let mut plans = self.plans.lock().unwrap();
        let id = destination.id();
        if let Some(p) = plans.get_mut(&plan_id) {
            p.add_destination(destination);
            p.connect_in_out(stop_id, id);
        }
    }

    pub fn start_plan_by_name(&mut self, name: String) {
        let mut lock = self.plans.lock().unwrap();
        let plan = lock
            .iter_mut()
            .filter(|(_id, plan)| plan.name == name)
            .map(|(_, plan)| plan)
            .next();
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
                match p.operate() {
                    Ok(_) => {}
                    Err(err) => error!("{}", err),
                }
            }
        }
    }

    pub fn stop_plan_by_name(&mut self, name: String) {
        let mut lock = self.plans.lock().unwrap();
        let plan = lock
            .iter_mut()
            .filter(|(_id, plan)| plan.name == name)
            .map(|(_, plan)| plan)
            .next();
        match plan {
            None => {}
            Some(p) => {
                p.halt();
            }
        }
    }

    pub fn attach(
        &mut self,
        source_id: usize,
        plan_id: usize,
        stop_id: usize,
    ) -> Result<(u16, u16), String> {
        {
            let attach = self.attachments.lock().unwrap();
            let values = attach.get(&source_id);
            if let Some(attachment) = values {
                return Ok((attachment.data_port, attachment.watermark_port));
            }
        }

        let data_port = 3131;
        let watermark_port = 4141;

        let tx = self.start_http_attacher(source_id, data_port, watermark_port);
        let mut lock = self.plans.lock().unwrap();
        let plan = lock.get_mut(&plan_id).unwrap();
        match plan.stations.get_mut(&stop_id) {
            None => error!("Could not find plan"),
            Some(station) => {
                station
                    .control
                    .0
                    .send(Command::Attach(source_id, tx))
                    .unwrap();
            }
        }
        Ok((data_port, watermark_port))
    }

    pub fn detach(&mut self, source_id: usize, plan_id: usize, stop_id: usize) {
        let attach = self.attachments.lock().unwrap();
        let values = attach.get(&source_id);
        if values.is_none() {
            return;
        }

        let mut lock = self.plans.lock().unwrap();
        let plan = lock.get_mut(&plan_id).unwrap();
        match plan.stations.get_mut(&stop_id) {
            None => error!("Could not find plan"),
            Some(station) => {
                station.control.0.send(Command::Detach(source_id)).unwrap();
            }
        }
        let mut lock = self.attachments.lock().unwrap();
        if lock.remove(&source_id).is_none() {
            error!("Could not remove")
        };
    }
}
