use crate::http::destination::DestinationState;
use crate::http::util;
use crate::processing::destination::Destination;
use crate::processing::plan::Status;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::{transform, Plan, Train};
use crate::util::new_channel;
use crate::util::Tx;
use axum::routing::get;
use axum::Router;
use crossbeam::channel::{RecvTimeoutError};
use schemas::message_generated::protocol::Create;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tower_http::cors::CorsLayer;
use tracing::{debug, error};

#[derive(Default)]
pub struct Storage {
    pub plans: Mutex<HashMap<usize, Plan>>,
    pub ins: Mutex<HashMap<String, fn(Map<String, Value>) -> Box<dyn Source>>>,
    pub outs: Mutex<HashMap<String, fn(Map<String, Value>) -> Box<dyn Destination>>>,
    pub transforms: Mutex<HashMap<String, fn(String, Value) -> Box<transform::Transform>>>,
    pub attachments: Mutex<HashMap<usize, Attachment>>,
}

pub struct Attachment {
    port: u16,
    sender: Tx<Train>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_channel: Sender<bool>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl Attachment {
    pub fn new(
        port: u16,
        sender: Tx<Train>,
        shutdown_channel: Sender<bool>,
        shutdown_flag: Arc<AtomicBool>,
        handles: Vec<thread::JoinHandle<()>>,
    ) -> Self {
        Attachment {
            port,
            sender,
            shutdown_flag,
            shutdown_channel,
            handles,
        }
    }
}

impl Storage {
    pub(crate) fn new() -> Storage {
        Default::default()
    }

    pub fn start_http_attacher(&mut self, id: usize, port: u16) -> Tx<Train> {
        let addr = util::parse_addr("127.0.0.1", port);
        let (tx, rx) = new_channel::<Train, &str>("HTTP Attacher Bus");

        let bus = Arc::new(Mutex::new(HashMap::<usize, Tx<Train>>::new()));
        let clone = bus.clone();

        let (sh_tx, sh_rx) = oneshot::channel();

        let shutdown_flag = Arc::new(AtomicBool::new(false));

        let flag = shutdown_flag.clone();

        let mut handles: Vec<JoinHandle<()>> = vec![];
        
        let res = thread::Builder::new()
            .name(format!("HTTP Observer {id}"))
            .spawn(move || {
                while !flag.load(Ordering::Relaxed) {
                    match rx.recv_timeout(Duration::from_millis(10)) {
                        Ok(train) => match clone.lock() {
                            Ok(lock) => {
                                lock.values().for_each(|l| match l.send(train.clone()) {
                                    Ok(_) => {}
                                    Err(err) => error!("Bus error: {err}"),
                                });
                            }
                            Err(e) => {
                                error!("Bus error lock: {e}");
                            }
                        },
                        Err(err) => match err {
                            RecvTimeoutError::Timeout => {
                                debug!("Error {err}")
                            }
                            RecvTimeoutError::Disconnected => {
                                error!(err = ?err, "recv channel error");
                            }
                        },
                    };
                }
            });

        match res {
            Ok(handle) => handles.push(handle), 
            Err(err) => error!("{}", err),
        }

        tokio::spawn(async move {
            let state = DestinationState { name: "HTTP Attacher".to_string(), outs: bus };
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

        let attachment = Attachment::new(port, tx.clone(), sh_tx, shutdown_flag, handles);
        
        self.attachments.lock().unwrap().insert(id, attachment);
        tx
    }

    pub fn create_plan(&mut self, create: Create) -> Result<(), String> {
        let create_plan = create.create_type_as_create_plan().unwrap();
        if create_plan.name().is_some() && create_plan.plan().is_some() {
            let plan = Plan::parse(create_plan.plan().unwrap());

            let mut plan = match plan {
                Ok(plan) => plan,
                Err(_) => todo!(),
            };

            plan.set_name(create_plan.name().unwrap().to_string());
            self.add_plan(plan);
            Ok(())
        } else {
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

    pub fn add_destination(
        &mut self,
        plan_id: usize,
        stop_id: usize,
        destination: Box<dyn Destination>,
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

    pub fn attach(
        &mut self,
        source_id: usize,
        plan_id: usize,
        stop_id: usize,
    ) -> Result<usize, String> {
        {
            let attach = self.attachments.lock().unwrap();
            let values = attach.get(&source_id);
            if let Some(attachment) = values {
                return Ok(attachment.port as usize);
            }
        }

        let tx = self.start_http_attacher(source_id, 3131);
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
        Ok(3131)
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
        match lock.remove(&source_id) {
            None => error!("Could not remove"),
            Some(_) => {}
        };
    }
}
