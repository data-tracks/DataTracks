use crate::{Time, Value};
use core::fmt::{Display, Formatter};
use flatbuffers::FlatBufferBuilder;
use redb::TypeName;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::collections::BTreeMap;
use std::ops;
use track_rails::message_generated::protocol::{
    Message, MessageArgs, OkStatus, OkStatusArgs, Payload, Status, Train as FlatTrain, TrainArgs,
};

#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Serialize,
    Writable,
    Readable,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Hash,
)]
pub struct TrainId(usize, usize);

impl TrainId {
    pub fn new(part_id: usize, id: usize) -> Self {
        TrainId(part_id, id)
    }
}

impl Display for TrainId {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.write_fmt(format_args!("{}{}", self.0, self.1))
    }
}

pub type MutWagonsFunc = Box<dyn FnMut(&mut Vec<Train>) -> Train>;

#[derive(Clone, Debug, Deserialize, Serialize, Writable, Readable)]
pub struct Train {
    pub marks: BTreeMap<usize, Time>,
    pub values: Vec<Value>,
    pub event_time: Time,
    pub id: TrainId,
}

impl Train {
    pub fn new(values: Vec<Value>, part_id: usize) -> Self {
        Train {
            marks: BTreeMap::new(),
            values,
            event_time: Time::now(),
            id: TrainId(0, part_id),
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
        self.values = self.values.into_iter().map(|v| v.wagonize(stop)).collect();
        self
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.values.extend(other.values);

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

    fn add(mut self, mut rhs: Train) -> Self::Output {
        self.values.append(&mut rhs.values);
        self
    }
}

impl From<Train> for Vec<u8> {
    fn from(value: Train) -> Self {
        let mut builder = FlatBufferBuilder::new();

        let args = TrainArgs {
            values: {
                Some({
                    let values = value
                        .values
                        .iter()
                        .map(|e| e.flatternize(&mut builder))
                        .collect::<Vec<_>>();
                    builder.create_vector(values.as_slice())
                })
            },
            topic: None,
            event_time: None,
        };
        let data = FlatTrain::create(&mut builder, &args);

        let status = OkStatus::create(&mut builder, &OkStatusArgs {}).as_union_value();

        let message = Message::create(
            &mut builder,
            &MessageArgs {
                data_type: Payload::Train,
                data: Some(data.as_union_value()),
                status_type: Status::OkStatus,
                status: Some(status),
            },
        );

        builder.finish(message, None);
        let train = builder.finished_data();

        train.to_vec()
    }
}

impl TryFrom<track_rails::message_generated::protocol::Train<'_>> for Train {
    type Error = String;

    fn try_from(
        value: track_rails::message_generated::protocol::Train<'_>,
    ) -> Result<Self, Self::Error> {
        let _topic = value.topic();

        Ok(Train::new(
            value
                .values()
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<_, _>>()?,
            0,
        ))
    }
}

impl From<&mut Train> for Train {
    fn from(other: &mut Train) -> Self {
        let mut train = Train::new(other.values.clone(), 0);
        train.id = other.id;
        train.marks = train.marks.iter().map(|(k, v)| (*k, *v)).collect();
        train
    }
}

impl From<Vec<Train>> for Train {
    fn from(wagons: Vec<Train>) -> Self {
        if wagons.len() == 1 {
            return wagons[0].clone();
        }

        let mut values = vec![];
        let mut part_id = 0usize;
        for train in wagons {
            part_id = train.id.1;
            values.append(train.values.clone().as_mut());
        }

        Train::new(values, part_id)
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

impl PartialEq<Self> for Train {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Train {}
