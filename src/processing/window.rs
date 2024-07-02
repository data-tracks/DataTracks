use chrono::NaiveTime;

use crate::processing::transform::Taker;
use crate::processing::window::Window::{Back, Interval};
use crate::util::TimeUnit;

#[derive(Clone)]
pub enum Window {
    Back(BackWindow),
    Interval(IntervalWindow),
}


impl Default for Window {
    fn default() -> Self {
        Back(BackWindow::new(0, TimeUnit::Millis))
    }
}


impl Window {
    pub(crate) fn windowing(&self) -> Taker {
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


#[derive(Clone)]
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


    pub(crate) fn get_window(&self) -> Taker {
        |trains| trains
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

#[derive(Clone)]
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

    pub(crate) fn get_window(&self) -> Taker {
        |trains| trains
    }
}

fn parse_time(time_str: &str) -> Result<NaiveTime, chrono::ParseError> {
    NaiveTime::parse_from_str(time_str, "%H:%M")
}


#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crossbeam::channel::unbounded;

    use crate::processing::station::Station;
    use crate::processing::train::Train;
    use crate::util::new_channel;
    use crate::value::Value;

    #[test]
    fn default_behavior() {
        let mut station = Station::new(0);

        let control = unbounded();

        let values = vec![Value::float(3.3), Value::int(3)];

        let (tx, num, rx) = new_channel();


        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0));
        station.send(Train::new(0, values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(mut t) => {
                assert_eq!(values.len(), t.values.clone().map_or(usize::MAX, |vec: Vec<Value>| vec.len()));
                for (i, value) in t.values.take().unwrap().into_iter().enumerate() {
                    assert_eq!(value, values[i]);
                    assert_ne!(Value::text(""), value)
                }
            }
            Err(..) => assert!(false),
        }
    }
}