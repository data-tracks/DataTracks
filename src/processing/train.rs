use crate::value::{Time, Value};
use schemas::message_generated::protocol::{Value as Val, ValueWrapper};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::collections::HashMap;

pub type MutWagonsFunc = Box<dyn FnMut(&mut Vec<Train>) -> Train>;

#[derive(Clone, Debug, Deserialize, Serialize, Writable, Readable)]
pub struct Train {
    pub marks: HashMap<usize, Time>,
    pub values: Option<Vec<Value>>,
}

impl Train {
    pub fn new(values: Vec<Value>) -> Self {
        Train { marks: HashMap::new(), values: Some(values) }
    }
    
    pub fn mark(self, stop: usize) -> Self {
        self.mark_timed(stop, Time::now())
    }
    
    pub fn mark_timed(mut self, stop: usize, time: Time) -> Self {
        self.marks.insert(stop, time);
        self
    }
    
    pub fn last(&self) -> usize {
        self.marks.iter().last().map(|(key,_)| *key).unwrap_or_default()
    }
    
}

impl TryFrom<schemas::message_generated::protocol::Train<'_>> for Train {
    type Error = String;

    fn try_from(value: schemas::message_generated::protocol::Train<'_>) -> Result<Self, Self::Error> {
        let _topic = value.topic();

        match value.values() {
            None => {
                Ok(Train::new(vec![]))
            }
            Some(values) => {
                Ok(Train::new(values.iter().map(|v| v.try_into()).collect::<Result<_, _>>()?))
            }
        }
    }
}

impl TryFrom<ValueWrapper<'_>> for Value {
    type Error = String;

    fn try_from(value: ValueWrapper) -> Result<Self, Self::Error> {
        match value.data_type() {
            Val::Time => {
                let time = value.data_as_time().ok_or("Could not find time")?;
                Ok(Value::time(time.data() as usize, 0))
            }
            Val::Text => {
                let string = value.data_as_text().ok_or("Could not find string")?;
                Ok(string.data().ok_or("Could not find string")?.into())
            }
            Val::Float => {
                let float = value.data_as_float().ok_or("Could not find float")?;
                Ok(Value::float(float.data() as _))
            }
            Val::Null => Ok(Value::null()),
            Val::Integer => {
                let integer = value.data_as_integer().ok_or("Could not find integer")?;
                Ok(Value::int(integer.data() as i64))
            }
            Val::List => {
                let list = value.data_as_list().ok_or("Could not find list")?;
                let list = list.data().ok_or("Could not find list")?;
                Ok(Value::array(list.iter().map(|v| v.try_into()).collect::<Result<_, _>>()?))
            }
            Val::Document => {
                todo!()
            },
            _ => panic!()
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
            return wagons[0].clone()
        }

        let mut values = vec![];
        for mut train in wagons {
            values.append(train.values.take().unwrap().as_mut());
        }

        let train = Train::new( values);
        train
    }
}
