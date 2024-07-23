use crate::value::Dict;

pub type MutWagonsFunc = Box<dyn FnMut(&mut Vec<Train>)>;

#[derive(Clone)]
pub struct Train {
    pub last: i64,
    pub values: Option<Vec<Dict>>,
}

impl Train {
    pub(crate) fn new(stop: i64, values: Vec<Dict>) -> Self {
        Train { last: stop, values: Some(values) }
    }


    pub(crate) fn set_last(&mut self, stop: i64) {
        self.last = stop;
    }
}

impl From<&mut Train> for Train {
    fn from(train: &mut Train) -> Self {
        Train::new(train.last, train.values.take().unwrap())
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

        Train::new(0, values)
    }
}
