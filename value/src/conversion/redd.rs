use std::cmp::Ordering;
use redb::{Key, TypeName};
use speedy::{Readable, Writable};
use crate::Value;

impl redb::Value for Value {
    type SelfType<'a>
        = Value
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Value::read_from_buffer(data).expect("Failed to deserialize Value")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.write_to_vec().expect("Failed to serialize Value")
    }

    fn type_name() -> TypeName {
        TypeName::new("value")
    }
}

impl Key for Value {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let val1: Value = Value::read_from_buffer(data1).expect("Failed to deserialize Value");
        let val2: Value = Value::read_from_buffer(data2).expect("Failed to deserialize Value");
        val1.cmp(&val2)
    }
}