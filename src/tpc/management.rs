use crate::management::{Storage, API};
use crate::processing::station::Command;
use crate::tpc::server::{StreamUser, TcpStream};
use crate::tpc::Server;
use crate::util::deserialize_message;
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info};

pub fn start_tpc(url: String, port: u16, storage: Arc<Mutex<Storage>>) {
    spawn(move || startup(url, port, storage));
    debug!("Startup done.")
}

fn startup(url: String, port: u16, storage: Arc<Mutex<Storage>>) {
    let (tx, rx) = unbounded();
    let server = Server::new(url.clone(), port);
    let management = TpcManagement {
        interrupt: tx,
        control: rx,
        api: Arc::new(Mutex::new(API::default())),
        storage: Arc::clone(&storage),
    };
    info!(
        "DataTracks (TrackRails) protocol listening on: http://localhost:{}",
        port
    );
    match server.start(management) {
        Ok(_) => {}
        Err(_) => {}
    }
}

#[derive(Clone)]
pub struct TpcManagement {
    interrupt: Sender<Command>,
    control: Receiver<Command>,
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
                                Ok(res) => stream.write_all(&res).await.unwrap(),
                                Err(err) => stream.write_all(&err).await.unwrap(),
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
