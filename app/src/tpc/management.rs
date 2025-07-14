use crate::management::{Api, Storage};
use crate::processing::station::Command;
use crate::tpc::server::{StreamUser, TcpStream};
use crate::tpc::Server;
use crate::util::deserialize_message;
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

pub fn start_tpc(url: String, port: u16, storage: Arc<Mutex<Storage>>) {
    let res = thread::Builder::new()
        .name("TPC Interface".to_string())
        .spawn(move || startup(url, port, storage));
    match res {
        Ok(_) => {}
        Err(err) => error!("{}", err),
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
        api: Arc::new(Mutex::new(Api::admin())),
        storage: Arc::clone(&storage),
    };
    info!(
        "DataTracks (TrackRails) protocol listening on: http://localhost:{}",
        port
    );
    match server.start(management, tx, rx) {
        Ok(_) => {}
        Err(err) => error!("{}", err),
    }
}

#[derive(Clone)]
pub struct TpcManagement {
    interrupt: Arc<Sender<Command>>,
    control: Arc<Receiver<Command>>,
    storage: Arc<Mutex<Storage>>,
    api: Arc<Mutex<Api>>,
}

impl StreamUser for TpcManagement {
    async fn handle(&mut self, mut stream: TcpStream) {
        let mut len_buf = [0u8; 4];

        loop {
            match stream.read_exact(&mut len_buf).await {
                Ok(()) => {
                    let size = u32::from_be_bytes(len_buf) as usize;

                    let mut buffer = vec![0; size];

                    if let Err(err) = stream.read_exact(&mut buffer).await {
                        warn!("error on reading stream {}", err);
                    };

                    match deserialize_message(&buffer) {
                        Ok(msg) => {
                            match Api::handle_message(self.storage.clone(), self.api.clone(), msg) {
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
                        Err(e) => {
                            warn!("could not deserialize message {}", e);
                        }
                    }
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

#[cfg(test)]
mod test {
    use crate::util::deserialize_message;
    use flatbuffers::FlatBufferBuilder;
    use track_rails::message_generated::protocol::{
        Message, MessageArgs, OkStatus, OkStatusArgs, Payload, RegisterRequest,
        RegisterRequestArgs, Status,
    };

    #[test]
    fn register_serialize() {
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
        let msg = builder.finished_data().to_vec();

        let msg = deserialize_message(msg.as_slice()).unwrap();
    }
}
