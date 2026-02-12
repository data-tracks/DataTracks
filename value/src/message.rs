use crate::{Value, flatbuf as fb};
use anyhow::anyhow;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

pub struct Message {
    pub topics: Vec<String>,
    pub payload: Vec<Value>,
    pub timestamp: i64,
}

impl Message {
    /// SERIALIZE
    /// Convert Rust Message -> FlatBuffers Bytes
    pub fn pack(&self) -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::with_capacity(1024);

        // 1. Build the recursive Value payload first
        let payload_offsets: Vec<WIPOffset<fb::Value>> = self
            .payload
            .iter()
            .map(|v| v.to_fb_offset(&mut fbb))
            .collect();
        let payloads_vec = fbb.create_vector(&payload_offsets);

        // 2. Build the topics vector
        let topic_offsets: Vec<WIPOffset<&str>> =
            self.topics.iter().map(|s| fbb.create_string(s)).collect();
        let topics_vec = fbb.create_vector(&topic_offsets);

        // 3. Create the Message table
        let root = fb::Message::create(
            &mut fbb,
            &fb::MessageArgs {
                topics: Some(topics_vec),
                payload: Some(payloads_vec),
                timestamp: self.timestamp,
            },
        );

        fbb.finish(root, None);
        fbb.finished_data().to_vec()
    }

    /// DESERIALIZE
    /// Convert FlatBuffers Bytes -> Rust Message
    pub fn unpack(buffer: &[u8]) -> anyhow::Result<Self> {
        let fb_msg = fb::root_as_message(buffer)
            .map_err(|e| anyhow!("FlatBuffers verification failed: {:?}", e))?;

        // Extract Topics
        let mut topics = Vec::new();
        if let Some(fb_topics) = fb_msg.topics() {
            for t in fb_topics.iter() {
                topics.push(t.to_string());
            }
        }

        // Extract Payload
        let mut payload = Vec::new();
        if let Some(pl) = fb_msg.payload() {
            for v in pl.iter() {
                payload.push(Value::try_from(v)?);
            }
        }

        Ok(Message {
            topics,
            payload,
            timestamp: fb_msg.timestamp(),
        })
    }
}
