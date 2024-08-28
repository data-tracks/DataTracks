use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::util::{Tx, GLOBAL_ID};
use crate::value::{Dict, Value};
use crossbeam::channel::{unbounded, Sender};
use mqtt_packet_3_5::{MqttPacket, PacketDecoder, PublishPacket};
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;
use tracing::{debug, warn};

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
        let outs = self.outs.clone();
        let port = self.port;
        thread::spawn(move || {
            rt.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:".to_owned() + &port.to_string()).unwrap();
                while let Ok((stream, _)) = listener.accept() {
                    handle_message(Packet::new(stream), &outs);
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

    fn serialize(&self) -> SourceModel {
        SourceModel { type_name: String::from("Mqtt"), id: self.id.to_string(), configs: vec![] }
    }

    fn serialize_default() -> Option<SourceModel> {
        Some(SourceModel { type_name: String::from("Mqtt"), id: String::from("Mqtt"), configs: vec![] })
    }
}

struct Packet {
    stream: TcpStream,
    protocol: u8,
}

impl Packet {
    fn new(stream: TcpStream) -> Self {
        Packet { stream, protocol: 5 }
    }

    fn new_decoder(&self) -> PacketDecoder<TcpStream> {
        PacketDecoder::from_stream(self.stream.try_clone().unwrap())
    }

    fn write(&mut self, message: &Vec<u8>) {
        self.stream.write(message).unwrap();
    }
}

impl From<PublishPacket> for Dict {
    fn from(value: PublishPacket) -> Self {
        let mut map = BTreeMap::new();
        map.insert(String::from("topic"), Value::text(&value.topic));
        map.insert(String::from("data"), transform_binary(value.payload));
        Dict(map)
    }
}

fn handle_message(mut initial_packet: Packet, outs: &HashMap<i64, Tx<Train>>) {
    debug!("New MQTT client connected: {:?}", initial_packet.stream.peer_addr());
    let mut decoder = initial_packet.new_decoder();
    while decoder.has_more() {
        match decoder.decode_packet(initial_packet.protocol) {
            Ok(packet) => {
                match packet {
                    MqttPacket::Connect(connect) => {
                        debug!("Packet {:?} was connect", connect);
                        let connack = vec![
                            0x20, // type == CONNACK
                            0x02, // remaining length
                            0x00,
                            0x00  // accept
                        ];
                        initial_packet.write(&connack);
                        initial_packet.protocol = connect.protocol_version;
                        debug!("Message acknowledged");
                    }
                    MqttPacket::Subscribe(subscribe) => {
                        debug!("Packet {:?} was subscribed", subscribe)
                    }
                    MqttPacket::Publish(publish) => {
                        debug!("Packet {:?} was published", publish);
                        send_message(publish.into(), outs);
                    }
                    MqttPacket::Disconnect(disconnect) => {
                        debug!("Packet {:?} was disconnected", disconnect)
                    }
                    packet => debug!("Packet not yet supported {packet:?}")
                }
            }
            Err(e) => warn!("Could not read MQTT message.")
        }
    }
}

fn send_message(dict: Dict, outs: &HashMap<i64, Tx<Train>>) {
    let train = Train::new(-1, vec![dict]);
    for (_stop, tx) in outs {
        tx.send(train.clone()).unwrap();
    }
}

fn transform_binary(data: Vec<u8>) -> Value {
    Value::text(&String::from("test"))
}