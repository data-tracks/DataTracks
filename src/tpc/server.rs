use crate::processing::station::Command;
use crossbeam::channel::{Receiver, Sender};

use std::io::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tracing::info;
use crate::management::{Storage, API};

pub struct Server {
    addr: SocketAddr,
}

pub trait StreamUser:Clone {
    fn handle(&mut self, stream: TcpStream) -> impl std::future::Future<Output = ()> + Send;

    fn interrupt(&mut self) -> Receiver<Command>;

    fn control(&mut self) -> Sender<Command>;
}

impl Server {
    pub(crate) fn new(url: String, port: u16) -> Server {
        let addr = (url, port).to_socket_addrs().ok().unwrap().next().unwrap();
        Server { addr }
    }

    pub fn start(
        &self,
        user: impl StreamUser,
    ) -> Result<(), Error> {
        let rt = Runtime::new()?;
        rt.block_on(async {
            let listener = TcpListener::bind(self.addr).await?;
            info!("TPC server listening...");

            loop {
                let (stream, _) = listener.accept().await?;
                user.clone().handle(stream).await;
            }
        })
    }

    fn would_block(err: &Error) -> bool {
        err.kind() == io::ErrorKind::WouldBlock
    }

    fn interrupted(err: &Error) -> bool {
        err.kind() == io::ErrorKind::Interrupted
    }
}


