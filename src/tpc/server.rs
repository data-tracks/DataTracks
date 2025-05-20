use crate::processing::station::Command;
use crossbeam::channel::{Receiver, Sender};

use std::io::{Error};
use std::net::{SocketAddr, ToSocketAddrs};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener};
use tokio::runtime::Runtime;
use tracing::info;

pub struct Server {
    addr: SocketAddr,
}

pub struct TcpStream(tokio::net::TcpStream);


impl TcpStream {
    pub async fn write_all<'a>(&'a mut self, msg: &'a [u8]) -> Result<(), String> {
        let length: [u8; 4] = (msg.len() as u32).to_be_bytes();
        // we write length first
        self.0.write_all(&length).await.unwrap();
        // then msg
        self.0.write_all(msg).await.map_err(|err| err.to_string())
    }

    pub async fn read_exact<'a>(&'a mut self, buf: &'a mut [u8]) -> Result<(), String> {
        let _length = (buf.len() as u32).to_be_bytes();
        let _read = self.0.read_exact(buf).await.map_err(|err| err.to_string())?;
        Ok(())
    }

    pub async fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Result<usize, String> {
        match self.0.read(buf).await{
            Ok(size) => Ok(size),
            Err(err) => Err(err.to_string())
        }
    }
}

impl From<tokio::net::TcpStream> for TcpStream {
    fn from(stream: tokio::net::TcpStream) -> Self {
        TcpStream(stream)
    }
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
                user.clone().handle(stream.into()).await;
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


