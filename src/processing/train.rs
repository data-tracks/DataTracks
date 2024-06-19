use tokio::io::AsyncReadExt;

use crate::value::Value;

#[derive(Clone)]
pub struct Train {
    pub last: i64,
    pub values: Option<Vec<Value>>,
}

impl Train {
    pub(crate) fn new(stop: i64, values: Vec<Value>) -> Self {
        Train { last: stop, values: Some(values) }
    }


    pub(crate) fn visit(&mut self, stop: i64) {
        self.last = stop;
    }
}

impl From<&mut Train> for Train {
    fn from(train: &mut Train) -> Self {
        Train::new(train.last, train.values.take().unwrap())
    }
}

impl From<&mut Vec<Train>> for Train {
    fn from(wagons: &mut Vec<Train>) -> Self {
        let mut values = vec![];
        for train in wagons {
            values.append(train.values.take().unwrap().as_mut());
        }

        Train::new(0, values)
    }
}
