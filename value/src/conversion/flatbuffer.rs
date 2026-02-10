
use crate::{flatbuf as fb, Array, Bool, Int, Text, Value};
use anyhow::{bail, Context};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use std::collections::BTreeMap;

impl Value {

    /// Internal recursive helper for bottom-up building
    pub(crate) fn to_fb_offset<'a>(&self, fbb: &mut FlatBufferBuilder<'a>) -> WIPOffset<fb::Value<'a>> {
        let (union_type, union_offset) = match self {
            Value::Int(i) => (
                fb::ValueData::Int,
                fb::Int::create(fbb, &fb::IntArgs { value: i.0 }).as_union_value(),
            ),
            Value::Text(t) => {
                let s = fbb.create_string(&t.0);
                (
                    fb::ValueData::Text,
                    fb::Text::create(fbb, &fb::TextArgs { value: Some(s) }).as_union_value(),
                )
            }
            Value::Bool(b) => (
                fb::ValueData::Bool,
                fb::Bool::create(fbb, &fb::BoolArgs { value: b.0 }).as_union_value(),
            ),
            Value::Array(a) => {
                let mut offsets = Vec::with_capacity(a.values.len());
                for v in &a.values {
                    offsets.push(v.to_fb_offset(fbb));
                }
                let v_offset = fbb.create_vector(&offsets);
                (
                    fb::ValueData::Array,
                    fb::Array::create(fbb, &fb::ArrayArgs { values: Some(v_offset) }).as_union_value(),
                )
            }
            Value::Dict(d) => {
                let mut entries = Vec::new();
                for (k, v) in &d.values {
                    let key_off = fbb.create_string(k);
                    let val_off = v.to_fb_offset(fbb);
                    entries.push(fb::DictEntry::create(fbb, &fb::DictEntryArgs {
                        key: Some(key_off),
                        value: Some(val_off),
                    }));
                }
                let e_offset = fbb.create_vector(&entries);
                (
                    fb::ValueData::Dict,
                    fb::Dict::create(fbb, &fb::DictArgs { entries: Some(e_offset) }).as_union_value(),
                )
            }
            Value::Node(n) => {
                let mut labels_off = Vec::new();
                for l in &n.labels { labels_off.push(fbb.create_string(&l.0)); }
                let labels_vec = fbb.create_vector(&labels_off);

                let mut props_off = Vec::new();
                for (k, v) in &n.properties {
                    let key_off = fbb.create_string(k);
                    let val_off = v.to_fb_offset(fbb);
                    props_off.push(fb::DictEntry::create(fbb, &fb::DictEntryArgs {
                        key: Some(key_off),
                        value: Some(val_off),
                    }));
                }
                let props_vec = fbb.create_vector(&props_off);

                (
                    fb::ValueData::Node,
                    fb::Node::create(fbb, &fb::NodeArgs {
                        id: n.id.0,
                        labels: Some(labels_vec),
                        properties: Some(props_vec),
                    }).as_union_value(),
                )
            }
            Value::Null => (fb::ValueData::NONE, WIPOffset::new(0)),
            Value::Float(f) => {
                let data = fb::Float::create(fbb, &fb::FloatArgs {
                    number: f.number,
                    shift: f.shift
                });
                (
                    fb::ValueData::Float,
                    data.as_union_value(),
                )
            }
            Value::Time(t) => {
                let data = fb::Time::create(fbb, &fb::TimeArgs {
                    ms: t.ms,
                    ns: t.ns
                });
                (
                    fb::ValueData::Time,
                    data.as_union_value(),
                )
            }
            Value::Date(d) => {
                let data = fb::Date::create(fbb, &fb::DateArgs {
                    days: d.days
                });
                (
                    fb::ValueData::Date,
                    data.as_union_value(),
                )
            }
            Value::Edge(e) => {
                let label = e.label.clone().map(|label| fbb.create_string(label.0.as_str()) );

                // Properties for Edge (following the same DictEntry logic)
                let mut props_off = Vec::new();
                for (k, v) in &e.properties {
                    let key_off = fbb.create_string(k);
                    let val_off = v.to_fb_offset(fbb);
                    props_off.push(fb::DictEntry::create(fbb, &fb::DictEntryArgs {
                        key: Some(key_off),
                        value: Some(val_off),
                    }));
                }
                let props_vec = fbb.create_vector(&props_off);

                let data = fb::Edge::create(fbb, &fb::EdgeArgs {
                    id: e.id.0,
                    label,
                    start_id: e.start,
                    end_id: e.end,
                    properties: Some(props_vec),
                });
                (
                    fb::ValueData::Edge,
                    data.as_union_value(),
                )
            }
        };

        fb::Value::create(fbb, &fb::ValueArgs {
            data_type: union_type,
            data: if union_type == fb::ValueData::NONE { None } else { Some(union_offset) },
        })
    }
}

impl<'a> TryFrom<fb::Value<'a>> for Value {
    type Error = anyhow::Error;

    fn try_from(fb_val: fb::Value<'a>) -> anyhow::Result<Self> {
        match fb_val.data_type() {
            fb::ValueData::Int => {
                let table = fb_val.data_as_int().context("Missing Int table")?;
                Ok(Value::Int(Int(table.value())))
            }
            fb::ValueData::Text => {
                let table = fb_val.data_as_text().context("Missing Text table")?;
                let s = table.value().context("Null string in Text table")?;
                Ok(Value::Text(Text(s.to_string())))
            }
            fb::ValueData::Bool => {
                let table = fb_val.data_as_bool().context("Missing Bool table")?;
                Ok(Value::Bool(Bool(table.value())))
            }
            fb::ValueData::Array => {
                let table = fb_val.data_as_array().context("Missing Array table")?;
                let fb_list = table.values().context("Array missing values vector")?;
                let mut vals = Vec::with_capacity(fb_list.len());
                for item in fb_list.iter() {
                    vals.push(Value::try_from(item)?);
                }
                Ok(Value::Array(Array::new(vals)))
            }
            fb::ValueData::Dict => {
                let table = fb_val.data_as_dict().context("Missing Dict table")?;
                let entries = table.entries().context("Dict missing entries vector")?;
                let mut map = BTreeMap::new();
                for entry in entries.iter() {
                    let key = entry.key().context("Dict entry missing key")?.to_string();
                    let val = Value::try_from(entry.value().context("Dict entry missing value")?)?;
                    map.insert(key, val);
                }
                Ok(Value::dict(map))
            }
            fb::ValueData::Node => {
                let table = fb_val.data_as_node().context("Missing Node table")?;

                let mut labels = Vec::new();
                if let Some(fb_labels) = table.labels() {
                    for l in fb_labels.iter() { labels.push(Text(l.to_string())); }
                }

                let mut props = BTreeMap::new();
                if let Some(fb_props) = table.properties() {
                    for p in fb_props.iter() {
                        let k = p.key().context("Node property missing key")?.to_string();
                        let v = Value::try_from(p.value().context("Node property missing value")?)?;
                        props.insert(k, v);
                    }
                }
                Ok(Value::node(Int(table.id()), labels, props))
            }
            fb::ValueData::NONE => Ok(Value::Null),
            fb::ValueData::Float => {
                let table = fb_val.data_as_float().context("Missing Float table")?;
                Ok(Value::float_parts(table.number(), table.shift()))
            }
            fb::ValueData::Time => {
                let table = fb_val.data_as_time().context("Missing Time table")?;
                Ok(Value::time(table.ms(), table.ns()))
            }
            fb::ValueData::Date => {
                let table = fb_val.data_as_date().context("Missing Date table")?;
                Ok(Value::date(table.days()))
            }
            fb::ValueData::Edge => {
                let table = fb_val.data_as_edge().context("Missing Edge table")?;
                let label = table.label().map(|label| Text(label.to_string()));

                let mut props = BTreeMap::new();
                if let Some(fb_props) = table.properties() {
                    for p in fb_props.iter() {
                        let k = p.key().context("Edge property missing key")?.to_string();
                        let v = Value::try_from(p.value().context("Edge property missing value")?)?;
                        props.insert(k, v);
                    }
                }
                Ok(Value::edge(Int(table.id()), label, table.start_id(), table.end_id(), props))
            }
            _ => bail!("Unsupported data type"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Message;
    use std::collections::BTreeMap;
    use std::vec;

    #[test]
    fn test_message_round_trip() {
        // 1. Create a complex Node structure (the payload)
        let mut properties = BTreeMap::new();
        properties.insert("email".to_string(), Value::text("alice@example.com"));
        properties.insert("active".to_string(), Value::bool(true));
        properties.insert("scores".to_string(), Value::array(vec![
            Value::int(10),
            Value::int(20)
        ]));

        let original_node = Value::node(
            Int(42),
            vec![Text("User".to_string()), Text("Admin".to_string())],
            properties,
        );

        // 2. Wrap it in a Message with multiple topics
        let original_msg = Message {
            topics: vec!["system.events".to_string(), "user.profile.update".to_string()],
            payload: vec![original_node],
            timestamp: 1700000000,
        };

        // 3. Serialize and Deserialize
        let buffer = original_msg.pack();
        let decoded_msg = Message::unpack(&buffer).expect("Failed to unpack message");

        // 4. Global Equality Check
        assert_eq!(original_msg.topics, decoded_msg.topics);
        assert_eq!(original_msg.timestamp, decoded_msg.timestamp);
        assert_eq!(original_msg.payload, decoded_msg.payload);

        // 5. Manual check for specific fields in the payload
        if let Value::Node(n) = &decoded_msg.payload[0] {
            assert_eq!(n.id.0, 42);
            assert_eq!(n.labels[0].0, "User");

            let email = n.properties.get("email").unwrap().as_text().unwrap();
            assert_eq!(email.0, "alice@example.com");
        } else {
            panic!("Decoded payload was not a Node");
        }
    }

    #[test]
    fn test_null_payload_with_topics() {
        let original_msg = Message {
            topics: vec!["heartbeat".to_string()],
            payload: vec![Value::Null],
            timestamp: 123456789,
        };

        let buffer = original_msg.pack();
        let decoded_msg = Message::unpack(&buffer).expect("Failed to unpack message");

        assert_eq!(decoded_msg.topics.len(), 1);
        assert_eq!(decoded_msg.topics[0], "heartbeat");
        assert_eq!(decoded_msg.payload[0], Value::Null);
    }
}