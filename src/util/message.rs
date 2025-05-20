use schemas::message_generated::protocol::Message;


pub fn deserialize_message(buf: &[u8]) -> Result<Message, String> {
    flatbuffers::root::<Message>(buf).map_err(|e| e.to_string())
}