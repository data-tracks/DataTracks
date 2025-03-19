use flatbuffers::FlatBufferBuilder;
use schemas::message_generated::protocol;
use schemas::message_generated::protocol::Message;

pub fn serialize_message(action: u32, data: &str) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();

    todo!();
}

pub fn deserialize_message(buf: &[u8]) -> Message {
    flatbuffers::root::<Message>(buf).unwrap()
}