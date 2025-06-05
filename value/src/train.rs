use crate::{Time, Value};
use flatbuffers::FlatBufferBuilder;
use redb::TypeName;
use schemas::message_generated::protocol::{
    Message, MessageArgs, Payload, Train as FlatTrain, TrainArgs,
};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::collections::HashMap;
use std::ops;

pub type MutWagonsFunc = Box<dyn FnMut(&mut Vec<Train>) -> Train>;

#[derive(Clone, Debug, Deserialize, Serialize, Writable, Readable)]
pub struct Train {
    pub marks: HashMap<usize, Time>,
    pub values: Option<Vec<Value>>,
    pub event_time: Time,
}

impl Train {
    pub fn new(values: Vec<Value>) -> Self {
        Train {
            marks: HashMap::new(),
            values: Some(values),
            event_time: Time::now(),
        }
    }

    pub fn mark(self, stop: usize) -> Self {
        self.mark_timed(stop, Time::now())
    }

    pub fn mark_timed(mut self, stop: usize, time: Time) -> Self {
        self.marks.insert(stop, time);
        self
    }

    pub fn flag(mut self, stop: usize) -> Self {
        self.values = self
            .values
            .map(|values| values.into_iter().map(|v| v.wagonize(stop)).collect());
        self
    }

    pub fn merge(mut self, other: Self) -> Self {
        if let Some(other_values) = other.values {
            match self.values.as_mut() {
                None => {}
                Some(values) => {
                    values.extend(other_values);
                }
            }
        }
        self
    }

    pub fn last(&self) -> usize {
        self.marks
            .iter()
            .last()
            .map(|(key, _)| *key)
            .unwrap_or_default()
    }
}

impl ops::Add<Train> for Train {
    type Output = Train;

    fn add(mut self, rhs: Train) -> Self::Output {
        self.values = match self.values {
            None => None,
            Some(mut values) => match rhs.values {
                None => None,
                Some(mut b) => {
                    values.append(&mut b);
                    Some(values)
                }
            },
        };
        self
    }
}

impl<'a> Into<Vec<u8>> for Train {
    fn into(self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();

        let args = TrainArgs {
            values: self.values.map(|v| {
                let values = &v
                    .iter()
                    .map(|e| e.flatternize(&mut builder))
                    .collect::<Vec<_>>();
                builder.create_vector(values)
            }),
            topic: None,
            event_time: None,
        };
        let data = FlatTrain::create(&mut builder, &args);

        let message = Message::create(
            &mut builder,
            &MessageArgs {
                data_type: Payload::Train,
                data: Some(data.as_union_value()),
                status: None,
            },
        );

        builder.finish(message, None);
        let train = builder.finished_data();

        train.to_vec()
    }
}

impl TryFrom<schemas::message_generated::protocol::Train<'_>> for Train {
    type Error = String;

    fn try_from(
        value: schemas::message_generated::protocol::Train<'_>,
    ) -> Result<Self, Self::Error> {
        let _topic = value.topic();

        match value.values() {
            None => Ok(Train::new(vec![])),
            Some(values) => Ok(Train::new(
                values
                    .iter()
                    .map(|v| v.try_into())
                    .collect::<Result<_, _>>()?,
            )),
        }
    }
}

impl From<&mut Train> for Train {
    fn from(train: &mut Train) -> Self {
        let mut train = Train::new(train.values.take().unwrap());
        train.marks = train.marks.iter().map(|(k, v)| (*k, v.clone())).collect();
        train
    }
}

impl From<Vec<Train>> for Train {
    fn from(wagons: Vec<Train>) -> Self {
        if wagons.len() == 1 {
            return wagons[0].clone();
        }

        let mut values = vec![];
        for mut train in wagons {
            values.append(train.values.take().unwrap().as_mut());
        }

        let train = Train::new(values);
        train
    }
}

impl redb::Value for Train {
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
        Value::read_from_buffer(data).expect("Failed to deserialize Train")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.write_to_vec().expect("Failed to serialize Value")
    }

    fn type_name() -> TypeName {
        TypeName::new("train")
    }
}
