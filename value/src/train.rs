use crate::event::Event;
use crate::{Time, Value};
use core::fmt::{Display, Formatter};
use flatbuffers::FlatBufferBuilder;
use redb::TypeName;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::collections::BTreeMap;
use std::{ops, vec};
use track_rails::message_generated::protocol::{
    Events, EventsArgs, Message, MessageArgs, OkStatus, OkStatusArgs, Payload, Status,
    Train as FlatTrain, TrainArgs, TrainContent as FlatTrainContent, TrainContentArgs, Values,
    ValuesArgs, ValuesOrEvents,
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
    pub content: TrainContent,
    pub event_time: Time,
    pub id: TrainId,
}

#[derive(Clone, Debug, Deserialize, Serialize, Writable, Readable)]
pub enum TrainContent {
    Values(Vec<Value>),
    Events(Vec<Event>),
}

impl TrainContent {

    pub fn into_values(self) -> Vec<Value> {
        self.into()
    }

    pub fn is_empty(&self) -> bool {
        match self {
            TrainContent::Values(v) => v.is_empty(),
            TrainContent::Events(e) => e.is_empty()
        }
    }

    pub fn len(&self) -> usize {
        match self {
            TrainContent::Values(v) => v.len(),
            TrainContent::Events(e) => e.len()
        }
    }
}

impl From<TrainContent> for Vec<Value> {
    fn from(value: TrainContent) -> Self {
        match value {
            TrainContent::Values(v) => v,
            TrainContent::Events(e) => e.into_iter().map(|e| e.into()).collect(),
        }
    }
}


impl Train {
    pub fn new(content: TrainContent, part_id: usize, id: usize) -> Self {
        Train {
            marks: BTreeMap::new(),
            content,
            event_time: Time::now(),
            id: TrainId(part_id, id),
        }
    }
    pub fn new_values(values: Vec<Value>, part_id: usize, id: usize) -> Self {
        Self::new(TrainContent::Values(values), part_id, id)
    }

    pub fn new_events(events: Vec<Event>, part_id: usize, id: usize) -> Self {
        Self::new(TrainContent::Events(events), part_id, id)
    }

    pub fn mark(self, stop: usize) -> Self {
        self.mark_timed(stop, Time::now())
    }

    pub fn mark_timed(mut self, stop: usize, time: Time) -> Self {
        self.marks.insert(stop, time);
        self
    }

    pub fn into_values(self) -> Vec<Value> {
        self.content.into()
    }

    pub fn len(&self) -> usize {
        self.content.len()
    }

    pub fn flag(mut self, stop: usize) -> Self {
        match self.content {
            TrainContent::Values(v) => {
                self.content =
                    TrainContent::Values(v.into_iter().map(|v| v.wagonize(stop)).collect());
            }
            TrainContent::Events(_) => {}
        }
        self
    }

    pub fn merge(mut self, other: Self) -> Self {
        match (self.content, other.content) {
            (TrainContent::Events(mut a), TrainContent::Events(b)) => {
                a.extend(b);
                self.content = TrainContent::Events(a);
            }
            (TrainContent::Values(mut a), TrainContent::Values(b)) => {
                a.extend(b);
                self.content = TrainContent::Values(a);
            }
            (_, _) => panic!("merge conflict"),
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

    fn add(self, rhs: Train) -> Self::Output {
        self.merge(rhs)
    }
}

impl From<Train> for Vec<u8> {
    fn from(value: Train) -> Self {
        let mut builder = FlatBufferBuilder::new();

        let args = match value.content {
            TrainContent::Values(v) => {
                let values = v
                    .iter()
                    .map(|e| e.flatternize(&mut builder))
                    .collect::<Vec<_>>();
                let values = builder.create_vector(values.as_slice());
                let values = Values::create(
                    &mut builder,
                    &ValuesArgs {
                        values: Some(values),
                    },
                )
                .as_union_value();

                FlatTrainContent::create(
                    &mut builder,
                    &TrainContentArgs {
                        data_type: ValuesOrEvents::Values,
                        data: Some(values),
                    },
                )
            }
            TrainContent::Events(v) => {
                let events = v
                    .iter()
                    .map(|e| e.flatternize(&mut builder))
                    .collect::<Vec<_>>();
                let events = builder.create_vector(events.as_slice());
                let events = Events::create(
                    &mut builder,
                    &EventsArgs {
                        events: Some(events),
                    },
                )
                .as_union_value();

                FlatTrainContent::create(
                    &mut builder,
                    &TrainContentArgs {
                        data_type: ValuesOrEvents::Values,
                        data: Some(events),
                    },
                )
            }
        };

        let event_time = value.event_time.flatternize(&mut builder);

        let args = TrainArgs {
            content: Some(args),
            topic: None,
            event_time: Some(event_time),
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

        let content = match value.content().data_type() {
            ValuesOrEvents::Events => {
                let events = value
                    .content()
                    .data_as_events()
                    .ok_or("No events present".to_string())?;
                let events = events
                    .events()
                    .iter()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<Event>, _>>()?;
                TrainContent::Events(events)
            }
            ValuesOrEvents::Values => {
                let values = value
                    .content()
                    .data_as_values()
                    .ok_or("No values present".to_string())?;
                let values = values
                    .values()
                    .iter()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<Value>, _>>()?;
                TrainContent::Values(values)
            }
            ValuesOrEvents(0_u8) | ValuesOrEvents(3_u8..=u8::MAX) => {
                return Err("Too many values specified".to_string());
            }
        };

        Ok(Train::new(content, 0, 0))
    }
}

impl From<&mut Train> for Train {
    fn from(other: &mut Train) -> Self {
        let mut train = Train::new(other.content.clone(), 0, 0);
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

        wagons.into_iter().reduce(|a, b| a.merge(b)).unwrap()
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

impl From<(usize, usize, Event)> for Train {
    // part_id, id, event
    fn from(value: (usize, usize, Event)) -> Self {
        let part_id = value.0;
        let id = value.1;
        let event = value.2;
        Train::new_events(vec![event], part_id, id)
    }
}

impl PartialEq<Self> for Train {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Train {}
