use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::ui::{ConfigModel, StringModel};
use crate::util::{Tx, GLOBAL_ID};
use crate::value::{Dict, Value};
use crossbeam::channel::{unbounded, Sender};
use mqtt_packet_3_5::{MqttPacket, PacketDecoder, PublishPacket};
use serde_json::Map;
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;
use tracing::{debug, warn};

pub struct MqttSource {
    id: i64,
    url: String,
    port: u16,
    outs: HashMap<i64, Tx<Train>>,
}


impl MqttSource {
    pub fn new(url: String, port: u16) -> Self {
        MqttSource { port, url, id: GLOBAL_ID.new_id(), outs: HashMap::new() }
    }
}

impl Configurable for MqttSource {
    fn get_name(&self) -> String {
        String::from("Mqtt")
    }

    fn get_options(&self) -> Map<String, serde_json::Value> {
        let mut options = serde_json::map::Map::new();
        options.insert("url".to_string(), serde_json::Value::String(self.url.clone()));
        options.insert("port".to_string(), serde_json::Value::Number(self.port.into()));
        options
    }
}

impl Source for MqttSource {
    fn parse(options: Map<String, serde_json::Value>) -> Result<Self, String> {
        let port = options.get("port").unwrap().as_u64().unwrap_or(9999);
        let url = options.get("url").unwrap().as_str().unwrap().parse().unwrap();
        Ok(MqttSource::new(url, port as u16))
    }

    fn operate(&mut self, _control: Arc<Sender<Command>>) -> Sender<Command> {
        let rt = Runtime::new().unwrap();
        debug!("starting mqtt source...");

        let (tx, _rx) = unbounded();
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

    fn get_id(&self) -> i64 {
        self.id
    }

    fn serialize(&self) -> SourceModel {
        SourceModel { type_name: String::from("Mqtt"), id: self.id.to_string(), configs: HashMap::new() }
    }

    fn from( configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String> {
        let port = match configs.get("port") {
            Some(port) => {
                match port {
                    ConfigModel::String(port) => {
                        port.string.parse::<u16>().unwrap()
                    }
                    ConfigModel::Number(port) => {
                        port.number as u16
                    }
                    _ => return Err(String::from("Could not create HttpSource."))
                }
            }
            _ => return Err(String::from("Could not create HttpSource."))
        };
        let url = match configs.get("url") {
            Some(ConfigModel::String(url)) => {
                url.string.clone()
            }
            _ => return Err(String::from("Could not create HttpSource."))
        };

        Ok(Box::new(MqttSource::new(url, port)))
    }

    fn serialize_default() -> Result<SourceModel, ()> {
        let mut configs = HashMap::new();
        configs.insert(String::from("port"), ConfigModel::String(StringModel::new("7777")));
        Ok(SourceModel { type_name: String::from("Mqtt"), id: String::from("Mqtt"), configs })
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

    fn write(&mut self, message: &[u8]) {
        self.stream.write_all(message).unwrap();
    }
}

impl From<PublishPacket> for Dict {
    fn from(value: PublishPacket) -> Self {
        let mut map = BTreeMap::new();
        map.insert(String::from("topic"), Value::text(&value.topic));
        map.insert(String::from("data"), transform_binary(value.payload));
        Dict::new(map)
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
            Err(e) => warn!("Could not read MQTT message. {}", e)
        }
    }
}

fn send_message(dict: Dict, outs: &HashMap<i64, Tx<Train>>) {
    let train = Train::new(-1, vec![Value::Dict(dict)]);
    for tx in outs.values() {
        tx.send(train.clone()).unwrap();
    }
}

fn transform_binary(_data: Vec<u8>) -> Value {
    Value::text(&String::from("test"))
}