use chrono::NaiveTime;

use crate::processing::train::Train;
use crate::processing::window::Window::{Back, Interval};
use crate::util::TimeUnit;

pub enum Window {
    Back(BackWindow),
    Interval(IntervalWindow),
}


impl Window {
    pub(crate) fn default() -> Self {
        Back(BackWindow::new(0, TimeUnit::Millis))
    }

    pub(crate) fn windowing(&self) -> Box<dyn Fn(Train) -> Train + Send + 'static> {
        match self {
            Back(w) => w.get_window(),
            Interval(w) => w.get_window()
        }
    }

    pub(crate) fn dump(&self) -> String {
        match self {
            Back(w) => w.dump(),
            Interval(w) => w.dump()
        }
    }

    pub(crate) fn parse(stencil: String) -> Self {
        if stencil.contains('@') {
            return Interval(IntervalWindow::parse(stencil));
        }
        return Back(BackWindow::parse(stencil));
    }
}


pub struct BackWindow {
    time: i64,
    time_unit: TimeUnit,
}

impl BackWindow {
    pub fn new(time: i64, time_unit: TimeUnit) -> Self {
        BackWindow { time, time_unit }
    }
    fn parse(stencil: String) -> Self {
        let (digit, time_unit) = parse_interval(stencil.as_str());

        BackWindow::new(digit, time_unit)
    }


    pub(crate) fn get_window(&self) -> Box<dyn Fn(Train) -> Train + Send> {
        Box::new(|train: Train| -> Train{ train })
    }

    pub(crate) fn dump(&self) -> String {
        if self.time == 0 {
            return "".to_string();
        }
        "(".to_string() + &self.time.to_string() + self.time_unit.into() + ")"
    }
}

fn parse_interval(stencil: &str) -> (i64, TimeUnit) {
    let mut temp = "".to_string();
    let mut digit: i64 = 0;
    let mut digit_passed: bool = false;
    for char in stencil.chars() {
        if !char.is_numeric() && !digit_passed {
            digit = temp.parse().unwrap();
            digit_passed = false;
            temp = "".to_string();
        }
        temp.push(char);
    }
    let time_unit = parse_time_unit(temp);
    (digit, time_unit)
}

fn parse_time_unit(time: String) -> TimeUnit {
    match TimeUnit::try_from(time.as_str()) {
        Ok(t) => t,
        Err(_) => todo!()
    }
}

pub struct IntervalWindow {
    time: i64,
    time_unit: TimeUnit,
    start: NaiveTime,
}

impl IntervalWindow {
    fn new(time: i64, time_unit: TimeUnit, start: NaiveTime) -> IntervalWindow {
        IntervalWindow { time, time_unit, start }
    }
    pub(crate) fn dump(&self) -> String {
        "(".to_string() + &self.time.to_string() + self.time_unit.into() + "@" + &self.start.format("%H:%M").to_string() + ")"
    }
    fn parse(input: String) -> IntervalWindow {
        match input.split_once('@') {
            None => todo!(),
            Some((interval, start)) => {
                let (time, time_unit) = parse_interval(interval);
                let start = parse_time(start).unwrap();

                IntervalWindow::new(time, time_unit, start)
            }
        }
    }

    pub(crate) fn get_window(&self) -> Box<dyn Fn(Train) -> Train + Send> {
        Box::new(|train: Train| {
            return train;
        })
    }
}

fn parse_time(time_str: &str) -> Result<NaiveTime, chrono::ParseError> {
    NaiveTime::parse_from_str(time_str, "%H:%M")
}


#[cfg(test)]
mod test {
    use std::sync::mpsc::channel;

    use crate::processing::station::Station;
    use crate::processing::train::Train;
    use crate::value::Value;

    #[test]
    fn default_behavior() {
        let mut station = Station::new(0);


        let values = vec![Value::float(3.3), Value::int(3)];

        let (tx, rx) = channel();

        station.add_out(0, tx).unwrap();
        station.operate();
        station.send(Train::single(values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(t) => {
                assert_eq!(values.len(), t.values.get(&0).unwrap().len());
                for (i, value) in t.values.get(&0).unwrap().into_iter().enumerate() {
                    assert_eq!(*value, values[i]);
                    assert_ne!(Value::text(""), *value)
                }
            }
            Err(..) => assert!(false),
        }
    }
}