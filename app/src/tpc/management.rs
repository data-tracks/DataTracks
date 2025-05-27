use crate::management::{Storage, API};
use crate::processing::station::Command;
use crate::tpc::server::{StreamUser, TcpStream};
use crate::tpc::Server;
use crate::util::deserialize_message;
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info};

pub fn start_tpc(url: String, port: u16, storage: Arc<Mutex<Storage>>) {
    let res = thread::Builder::new().name("TPC Interface".to_string()).spawn(move || startup(url, port, storage));
    match res {
        Ok(_) => {}
        Err(err) => error!("{}", err)
    }
    debug!("Startup done.")
}

fn startup(url: String, port: u16, storage: Arc<Mutex<Storage>>) {
    let (tx, rx) = unbounded();
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);
    
    let server = Server::new(url.clone(), port);
    let management = TpcManagement {
        interrupt: tx.clone(),
        control: rx.clone(),
        api: Arc::new(Mutex::new(API::default())),
        storage: Arc::clone(&storage),
    };
    info!(
        "DataTracks (TrackRails) protocol listening on: http://localhost:{}",
        port
    );
    match server.start(management, tx, rx ) {
        Ok(_) => {}
        Err(_) => {}
    }
}

#[derive(Clone)]
pub struct TpcManagement {
    interrupt: Arc<Sender<Command>>,
    control: Arc<Receiver<Command>>,
    storage: Arc<Mutex<Storage>>,
    api: Arc<Mutex<API>>,
}

impl StreamUser for TpcManagement {
    async fn handle(&mut self, mut stream: TcpStream) {
        let mut len_buf = [0u8; 4];

        loop {
            match stream.read_exact(&mut len_buf).await {
                Ok(()) => {
                    let size = u32::from_be_bytes(len_buf) as usize;
                    
                    let buffer = vec![0; size];
                    
                    match deserialize_message(&buffer) {
                        Ok(msg) => {
                            match API::handle_message(self.storage.clone(), self.api.clone(), msg) {
                                Ok(res) => match stream.write_all(&res).await {
                                    Ok(_) => {}
                                    Err(err) => error!("{}", err),
                                },
                                Err(err) => match stream.write_all(&err).await {
                                    Ok(_) => {}
                                    Err(err) => error!("{}", err),
                                },
                            }
                        }
                        Err(_) => (),
                    };
                }
                _ => {
                    sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    fn interrupt(&mut self) -> Receiver<Command> {
        todo!()
    }

    fn control(&mut self) -> Sender<Command> {
        todo!()
    }
}
