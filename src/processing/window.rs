use std::collections::VecDeque;
use std::time::Instant;
use chrono::{Duration, NaiveTime};
use crate::processing::Train;
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
    pub(crate) fn windowing(&self) -> Box<Taker> {
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
    duration: Duration,
    time: i64,
    time_unit: TimeUnit,
}

impl BackWindow {
    pub fn new(time: i64, time_unit: TimeUnit) -> Self {
        BackWindow { time, time_unit, duration: get_duration(time, time_unit) }
    }
    fn parse(stencil: String) -> Self {
        let (digit, time_unit) = parse_interval(stencil.as_str());

        BackWindow::new(digit, time_unit)
    }


    pub(crate) fn get_window(& self) -> Box<Taker> {
        let mut buffer = VecDeque::new();
        let duration = self.duration.clone();
        Box::new(|train| {
            let instant = Instant::now();
            buffer.push_back((instant, train.values.clone().unwrap()));

            let mut values = vec![];
            let mut new_buffer = VecDeque::new();
            for (i, value) in buffer {
                if instant.checked_duration_since(i).unwrap().as_millis() < duration.num_milliseconds() as u128 {
                    values.append(value.clone().as_mut());
                    new_buffer.push_back((i, value))
                }
            }
            buffer = new_buffer;

            &mut Train::new(0, values)
        })
    }

    pub(crate) fn dump(&self) -> String {
        if self.time == 0 {
            return "".to_string();
        }
        "[".to_string() + &self.time.to_string() + self.time_unit.into() + "]"
    }
}

fn get_duration(time: i64, time_unit: TimeUnit) -> Duration {
    return match time_unit {
        TimeUnit::Millis => Duration::milliseconds(time),
        TimeUnit::Seconds => Duration::seconds(time),
        TimeUnit::Minutes => Duration::minutes(time),
        TimeUnit::Hours => Duration::hours(time),
        TimeUnit::Days => Duration::days(time)
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

    pub(crate) fn get_window(&self) -> Box<Taker> {
        let mut buffer = VecDeque::new();
        Box::new(|train| {
            buffer.push_back(train);
            train
        })
    }
}

fn parse_time(time_str: &str) -> Result<NaiveTime, chrono::ParseError> {
    NaiveTime::parse_from_str(time_str, "%H:%M")
}


#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crossbeam::channel::unbounded;

    use crate::processing::station::Command::READY;
    use crate::processing::station::Station;
    use crate::processing::train::Train;
    use crate::processing::window::{BackWindow, Window};
    use crate::util::{new_channel, TimeUnit};
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

    #[test]
    fn back_behavior() {
        let mut station = Station::new(0);

        station.window = Window::Back(BackWindow::new(3, TimeUnit::Millis));

        let control = unbounded();

        let values = vec![Value::float(3.3), Value::int(3)];

        let (tx, num, rx) = new_channel();


        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0));
        // wait for read
        assert_eq!(READY(0), control.1.recv().unwrap());

        for value in &values {
            station.send(Train::new(0, vec![value.clone()])).unwrap();
        }

        let mut res = rx.recv().unwrap();
        assert_eq!(res.values.take().unwrap().get(0).unwrap(), values.get(0).unwrap());

        let mut res = rx.recv().unwrap();
        assert_eq!(res.values.take().unwrap(), values);

    }
}