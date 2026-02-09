use std::collections::BTreeMap;
use crate::{Array, Int, Text, Value};

// 1. CONVERT FROM CAPNP (Decoding)
impl<'a> TryFrom<crate::valuecp::Reader<'a>> for Value {
    type Error = capnp::Error;

    fn try_from(reader: crate::valuecp::Reader<'a>) -> Result<Self, Self::Error> {
        match reader.which()? {
            crate::valuecp::Which::Int(i) => Ok(Value::int(i)),
            crate::valuecp::Which::Float(f) => {
                let f_reader = f;
                Ok(Value::float_parts(f_reader.get_number(), f_reader.get_shift()))
            }
            crate::valuecp::Which::Bool(b) => Ok(Value::bool(b)),
            crate::valuecp::Which::Text(t) => Ok(Value::text(t?.to_string()?.as_str())),
            crate::valuecp::Which::Time(t) => {
                let t_reader = t;
                Ok(Value::time(t_reader.get_ms(), t_reader.get_ns()))
            }
            crate::valuecp::Which::Date(d) => Ok(Value::date(d)),
            crate::valuecp::Which::Array(a) => {
                let reader_list = a?;
                let mut vals = Vec::with_capacity(reader_list.len() as usize);
                for item in reader_list.iter() {
                    vals.push(Value::try_from(item)?);
                }
                Ok(Value::Array(Array::new(vals)))
            }
            crate::valuecp::Which::Dict(d) => {
                let entries = d?;
                let mut map = BTreeMap::new();
                for entry in entries.iter() {
                    let key = entry.get_key()?.to_string()?;
                    let val = Value::try_from(entry.get_value()?)?;
                    map.insert(key, val);
                }
                Ok(Value::dict(map))
            }
            crate::valuecp::Which::Node(n) => {
                let n_reader = n?;
                let mut labels = Vec::new();
                for l in n_reader.get_labels()?.iter() {
                    labels.push(Text(l?.to_string()?));
                }
                let mut props = BTreeMap::new();
                for p in n_reader.get_properties()?.iter() {
                    props.insert(p.get_key()?.to_string()?, Value::try_from(p.get_value()?)?);
                }
                Ok(Value::node(Int(n_reader.get_id()), labels, props))
            }
            crate::valuecp::Which::Null(()) => Ok(Value::Null),
            _ => Err(capnp::Error::failed("Unknown variant".to_string())),
        }
    }
}

// 2. CONVERT TO CAPNP (Encoding)
impl Value {
    pub fn fill_builder(&self, mut builder: crate::valuecp::Builder) {
        match self {
            Value::Int(i) => builder.set_int(i.0),
            Value::Float(f) => {
                let mut f_builder = builder.init_float();
                f_builder.set_number(f.number);
                f_builder.set_shift(f.shift);
            }
            Value::Bool(b) => builder.set_bool(b.0),
            Value::Text(t) => builder.set_text(&t.0),
            Value::Time(t) => {
                let mut t_builder = builder.init_time();
                t_builder.set_ms(t.ms);
                t_builder.set_ns(t.ns);
            }
            Value::Date(d) => builder.set_date(d.days),
            Value::Array(a) => {
                let mut list_builder = builder.init_array(a.values.len() as u32);
                for (i, val) in a.values.iter().enumerate() {
                    val.fill_builder(list_builder.reborrow().get(i as u32));
                }
            }
            Value::Dict(d) => {
                let mut dict_builder = builder.init_dict(d.len() as u32);
                for (i, (k, v)) in d.iter().enumerate() {
                    let mut entry = dict_builder.reborrow().get(i as u32);
                    entry.set_key(k);
                    v.fill_builder(entry.init_value());
                }
            }
            Value::Node(n) => {
                let mut nb = builder.init_node();
                nb.set_id(n.id.0);

                let mut labels_builder = nb.reborrow().init_labels(n.labels.len() as u32);
                for (i, l) in n.labels.iter().enumerate() {
                    labels_builder.set(i as u32, &l.0);
                }

                let mut props_builder = nb.init_properties(n.properties.len() as u32);
                for (i, (k, v)) in n.properties.iter().enumerate() {
                    let mut entry = props_builder.reborrow().get(i as u32);
                    entry.set_key(k);
                    v.fill_builder(entry.init_value());
                }
            }
            Value::Null => builder.set_null(()),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use capnp::message::Builder;
    use capnp::serialize_packed;
    use std::collections::BTreeMap;
    use crate::value_capnp;

    #[test]
    fn test_value_round_trip() {
        // 1. Create a complex Node structure using your high-level Value enum
        let mut properties = BTreeMap::new();
        properties.insert("email".to_string(), Value::text("alice@example.com"));
        properties.insert("active".to_string(), Value::bool(true));

        // Nested array inside the dictionary
        properties.insert("scores".to_string(), Value::array(vec![
            Value::int(10),
            Value::int(20)
        ]));

        let original_node = Value::node(
            Int(42),
            vec![Text("User".to_string()), Text("Admin".to_string())],
            properties,
        );

        // 2. SERIALIZE: Convert Rust Enum -> Cap'n Proto Bytes
        let mut message = Builder::new_default();
        {
            let value_builder = message.init_root::<value_capnp::value::Builder>();
            original_node.fill_builder(value_builder);
        }

        let mut buffer = Vec::new();
        serialize_packed::write_message(&mut buffer, &message).expect("Serialization failed");

        // 3. DESERIALIZE: Convert Cap'n Proto Bytes -> Rust Enum
        let reader = serialize_packed::read_message(
            &mut buffer.as_slice(),
            capnp::message::ReaderOptions::new()
        ).expect("Reading message failed");

        let value_reader = reader.get_root::<value_capnp::value::Reader>().expect("Root read failed");
        let decoded_value = Value::try_from(value_reader).expect("Decoding failed");

        // 4. ASSERT: Ensure the data survived the journey
        assert_eq!(original_node, decoded_value);

        // Manual check for specific fields
        if let Value::Node(n) = decoded_value {
            assert_eq!(n.id.0, 42);
            assert_eq!(n.labels[0].0, "User");

            let email = n.properties.get("email").unwrap().as_text().unwrap();
            assert_eq!(email.0, "alice@example.com");
        } else {
            panic!("Decoded value was not a Node");
        }
    }

    #[test]
    fn test_null_value() {
        let original = Value::Null;

        let mut message = Builder::new_default();
        original.fill_builder(message.init_root());

        let mut buffer = Vec::new();
        serialize_packed::write_message(&mut buffer, &message).unwrap();

        let reader = serialize_packed::read_message(&mut buffer.as_slice(), Default::default()).unwrap();
        let decoded = Value::try_from(reader.get_root::<value_capnp::value::Reader>().unwrap()).unwrap();

        assert_eq!(original, decoded);
    }
}