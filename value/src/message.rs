use anyhow::{anyhow, Context};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use crate::{flatbuf as fb, Value};

pub struct Message {
    pub topics: Vec<String>,
    pub payload: Value,
    pub timestamp: i64,
}

impl Message {
    /// SERIALIZE: Convert Rust Message -> FlatBuffers Bytes
    pub fn pack(&self) -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::with_capacity(1024);

        // 1. Build the recursive Value payload first
        let payload_offset = self.payload.to_fb_offset(&mut fbb);

        // 2. Build the topics vector
        let topic_offsets: Vec<WIPOffset<&str>> = self.topics
            .iter()
            .map(|s| fbb.create_string(s))
            .collect();
        let topics_vec = fbb.create_vector(&topic_offsets);

        // 3. Create the Message table
        let root = fb::Message::create(&mut fbb, &fb::MessageArgs {
            topics: Some(topics_vec),
            payload: Some(payload_offset),
            timestamp: self.timestamp,
        });

        fbb.finish(root, None);
        fbb.finished_data().to_vec()
    }

    /// DESERIALIZE: Convert FlatBuffers Bytes -> Rust Message
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
        let payload = Value::try_from(fb_msg.payload().context("Missing payload")?)?;

        Ok(Message {
            topics,
            payload,
            timestamp: fb_msg.timestamp(),
        })
    }
}