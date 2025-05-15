use std::sync::{Arc, Mutex};
use std::thread::spawn;
use crossbeam::channel::{unbounded, Receiver, Sender};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info};
use crate::management::{Storage, API};
use crate::processing::station::Command;
use crate::tpc::Server;
use crate::tpc::server::StreamUser;
use crate::util::deserialize_message;

pub fn start_tpc(url: String, port: u16, storage: Arc<Mutex<Storage>>){
    spawn(move||startup(url, port, storage));
    debug!("Startup done.")
}

fn startup(url: String, port: u16, storage: Arc<Mutex<Storage>>) {
    let (tx, rx) = unbounded();
    let server = Server::new(url.clone(), port);
    let management = TpcManagement{
        interrupt: tx,
        control: rx,
        api: Arc::new(Mutex::new(API::default())),
        storage: Arc::clone(&storage),
    };
    match server.start(management) {
        Ok(_) => {}
        Err(_) => {}
    }
}

#[derive(Clone)]
pub struct TpcManagement{
    interrupt: Sender<Command>,
    control: Receiver<Command>,
    storage: Arc<Mutex<Storage>>,
    api: Arc<Mutex<API>>,
}

impl StreamUser for TpcManagement {
    async fn handle(&mut self, mut stream: TcpStream) {
        let mut buffer = [0; 1024]; // Buffer for incoming data

        match stream.read(&mut buffer).await {
            Ok(size) if size > 0 => {
                // Deserialize FlatBuffers message
                match deserialize_message(&buffer[..size]) {
                    Ok(msg) => {
                        match API::handle_message(self.storage.clone(), self.api.clone(), msg) {
                            Ok(res) => stream.write_all(&res).await.unwrap(),
                            Err(err) => stream.write_all(&err).await.unwrap()
                        }
                    },
                    Err(_) => (),
                };
            }
            _ => {
                info!("Client disconnected or error occurred");
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