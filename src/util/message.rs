use flatbuffers::FlatBufferBuilder;
use schemas::message_generated::protocol::Message;

pub fn serialize_message(action: u32, data: &str) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();

    todo!();
}

pub fn deserialize_message(buf: &[u8]) -> Result<Message, String> {
    flatbuffers::root::<Message>(buf).map_err(|e| e.to_string())
}