use crate::processing::station::Command;
use crate::processing::Train;
use crate::util::{deserialize_message, serialize_message, Tx};
use crossbeam::channel::{Receiver, Sender};

use std::io::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tracing::{debug, info};


pub struct Server {
    id: usize,
    addr: SocketAddr,
}

impl Server {
    pub(crate) fn new(id: usize, url: String, port: u16) -> Server {
        let addr = (url, port).to_socket_addrs().ok().unwrap().next().unwrap();
        Server { id, addr }
    }

    pub fn start(
        &self,
        rx: Receiver<Command>,
        outs: Vec<Tx<Train>>,
        control: Arc<Sender<Command>>,
    ) -> Result<(), Error> {
        let rt = Runtime::new()?;
        rt.block_on(async {
            let listener = TcpListener::bind(self.addr).await?;
            info!("Server listening...");

            loop {
                let (stream, _) = listener.accept().await?;
                tokio::spawn(Server::handle_client(stream, outs.clone(), control.clone()));
            }
        })


    }

    async fn handle_client(mut stream: TcpStream, vec: Vec<Tx<Train>>, arc: Arc<Sender<Command>>) {
        let mut buffer = [0; 1024]; // Buffer for incoming data

        match stream.read(&mut buffer).await {
            Ok(size) if size > 0 => {
                // Deserialize FlatBuffers message
                let message = deserialize_message(&buffer[..size]);
                info!("Received message: id={:?}, text={:?}", message, message.data().unwrap());

                // Prepare response
                let response = serialize_message(2, "Hello from Rust Server!");
                stream.write_all(&response).await.unwrap();
            }
            _ => {
                info!("Client disconnected or error occurred");
            }
        }
    }

    fn would_block(err: &Error) -> bool {
        err.kind() == io::ErrorKind::WouldBlock
    }

    fn interrupted(err: &Error) -> bool {
        err.kind() == io::ErrorKind::Interrupted
    }
}


