use crate::processing::station::Command;
use crossbeam::channel::{Receiver, Sender};

use crate::util::deserialize_message;
use flatbuffers::FlatBufferBuilder;
use schemas::message_generated::protocol::{
    Message, MessageArgs, OkStatus, OkStatusArgs, Payload, RegisterRequest, RegisterRequestArgs,
    Status,
};
use std::io;
use std::io::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tracing::{error, info};

pub struct Server {
    addr: SocketAddr,
}

pub struct TcpStream(tokio::net::TcpStream);

impl TcpStream {
    pub async fn write_all<'a>(&'a mut self, msg: &'a [u8]) -> Result<(), String> {
        let length: [u8; 4] = (msg.len() as u32).to_be_bytes();
        // we write length first
        match self.0.write_all(&length).await {
            Ok(_) => {}
            Err(err) => return Err(err.to_string()),
        };
        // then msg
        self.0.write_all(msg).await.map_err(|err| err.to_string())
    }

    pub async fn read_exact<'a>(&'a mut self, buf: &'a mut [u8]) -> Result<(), String> {
        let _length = (buf.len() as u32).to_be_bytes();
        let _read = self
            .0
            .read_exact(buf)
            .await
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    pub async fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Result<usize, String> {
        match self.0.read(buf).await {
            Ok(size) => Ok(size),
            Err(err) => Err(err.to_string()),
        }
    }
}

impl From<tokio::net::TcpStream> for TcpStream {
    fn from(stream: tokio::net::TcpStream) -> Self {
        TcpStream(stream)
    }
}
pub trait StreamUser: Clone {
    fn handle(&mut self, stream: TcpStream) -> impl std::future::Future<Output = ()> + Send;

    fn interrupt(&mut self) -> Receiver<Command>;

    fn control(&mut self) -> Sender<Command>;
}

pub fn handle_register() -> Result<Vec<u8>, String> {
    let mut builder = FlatBufferBuilder::new();

    let register = RegisterRequest::create(
        &mut builder,
        &RegisterRequestArgs {
            id: None,
            catalog: None,
        },
    )
    .as_union_value();

    let status = OkStatus::create(&mut builder, &OkStatusArgs {}).as_union_value();

    let msg = Message::create(
        &mut builder,
        &MessageArgs {
            data_type: Payload::RegisterRequest,
            data: Some(register),
            status_type: Status::OkStatus,
            status: Some(status),
        },
    );

    builder.finish(msg, None);
    Ok(builder.finished_data().to_vec())
}
pub async fn ack(stream: &mut TcpStream) -> Result<(), String> {
    read(stream).await?;

    stream.write_all(&handle_register().unwrap()).await?;
    Ok(())
}
async fn read<'a>(stream: &mut TcpStream) -> Result<String, String> {
    let mut len_buf = [0u8; 4];

    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(err) => error!("TPC Destination Regist{:?}", err),
    }
    let size = u32::from_be_bytes(len_buf) as usize;
    let mut buffer = vec![0; size];
    stream.read(&mut buffer).await?;
    match deserialize_message(&buffer) {
        Ok(msg) => Ok(format!("{:?}", msg)),
        Err(err) => Err(format!("Cannot deserialize {:?}", err)),
    }
}

impl Server {
    pub(crate) fn new(url: String, port: u16) -> Server {
        let addr = (url, port).to_socket_addrs().ok().unwrap().next().unwrap();
        Server { addr }
    }

    pub fn start(
        &self,
        user: impl StreamUser,
        control: Arc<Sender<Command>>,
        rx: Arc<Receiver<Command>>,
    ) -> Result<(), String> {
        let rt = Runtime::new().map_err(|err| err.to_string())?;
        rt.block_on(async {
            let listener = TcpListener::bind(self.addr)
                .await
                .map_err(|err| err.to_string())?;
            info!("TPC server listening {}...", self.addr);
            let rx = Arc::new(rx);

            control.send(Command::Ready(0)).unwrap();

            loop {
                let (stream, _) = listener.accept().await.map_err(|err| err.to_string())?;

                if let Ok(cmd) = rx.try_recv() {
                    if let Command::Stop(s) = cmd {
                        return Err(format!("Stopped {}", s));
                    }
                }

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
