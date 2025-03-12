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
use tracing::debug;


pub struct Server {
    addr: SocketAddr,
}

impl Server {
    fn new(url: String, port: u16) -> Server {
        let addr = (url, port).to_socket_addrs().ok().unwrap().next().unwrap();
        Server { addr }
    }

    pub fn start(
        &self,
        id: usize,
        url: String,
        port: u16,
        rx: Receiver<Command>,
        outs: Vec<Tx<Train>>,
        control: Arc<Sender<Command>>,
    ) -> Result<(), Error> {
        let rt = Runtime::new()?;
        rt.block_on(async {
            let addr = (url, port).to_socket_addrs().ok().unwrap().next().unwrap();
            let listener = TcpListener::bind(addr).await?;
            println!("Server listening...");

            loop {
                let (stream, _) = listener.accept().await?;
                tokio::spawn(self.handle_client(stream, outs.clone(), control.clone()));
            }
        })


    }

    async fn handle_client(&self, mut stream: TcpStream) {
        let mut buffer = [0; 1024]; // Buffer for incoming data

        match stream.read(&mut buffer).await {
            Ok(size) if size > 0 => {
                // Deserialize FlatBuffers message
                let message = deserialize_message(&buffer[..size]);
                println!("Received message: id={}, text={}", message.action(), message.data().unwrap());

                // Prepare response
                let response = serialize_message(2, "Hello from Rust Server!");
                stream.write_all(&response).await.unwrap();
            }
            _ => {
                println!("Client disconnected or error occurred");
            }
        }
    }

    fn run(&self, id: usize, rx: Receiver<Command>, outs: Vec<Tx<Train>>, control: Arc<Sender<Command>>) -> Result<(), Error> {

    }


    /// Returns `true` if the connection is done.
    fn handle_connection_event(
        id: usize,
        outs: Arc<Vec<Tx<Train>>>,
        registry: &Registry,
        connection: &mut TcpStream,
        event: &Event,
    ) -> io::Result<bool> {

    }

    fn would_block(err: &Error) -> bool {
        err.kind() == io::ErrorKind::WouldBlock
    }

    fn interrupted(err: &Error) -> bool {
        err.kind() == io::ErrorKind::Interrupted
    }
}


