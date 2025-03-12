use flatbuffers::FlatBufferBuilder;
use schemas::message_generated::protocol;
use schemas::message_generated::protocol::Message;

pub fn serialize_message(action: u32, data: &str) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();

    let action = builder.create_string(&action.to_string());
    let data = builder.create_string(data);
    let message = Message::create(&mut builder, &protocol::MessageArgs {
        action: Some(action), data: Some(data)
    });

    builder.finish(message, None);
    builder.finished_data().to_vec() // Return serialized bytes
}

pub fn deserialize_message(buf: &[u8]) -> Message {
    flatbuffers::root::<Message>(buf).unwrap()
}