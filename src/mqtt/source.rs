use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::util::{new_channel, Tx, GLOBAL_ID};
use crossbeam::channel::{unbounded, Sender};
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;
use tracing::debug;

pub struct MqttSource {
    id: i64,
    port: u16,
    stop: i64,
    outs: HashMap<i64, Tx<Train>>,
}


impl MqttSource {
    pub fn new(stop: i64, port: u16) -> Self {
        MqttSource { port, stop, id: GLOBAL_ID.new_id(), outs: HashMap::new() }
    }
}


impl Source for MqttSource {
    fn operate(&mut self, _control: Arc<Sender<Command>>) -> Sender<Command> {
        let rt = Runtime::new().unwrap();
        debug!("starting mqtt source...");

        let (tx, rx) = unbounded();
        let port = self.port;
        thread::spawn(move || {
            rt.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:".to_owned() + &port.to_string()).unwrap();
                while let Ok((stream, _)) = listener.accept() {
                    debug!("New mqtt client connected: {:?}", stream.peer_addr());
                }
            });
        });
        tx
    }

    fn add_out(&mut self, id: i64, out: Tx<Train>) {
        self.outs.insert(id, out);
    }

    fn get_stop(&self) -> i64 {
        self.stop
    }

    fn get_id(&self) -> i64 {
        self.id
    }
}