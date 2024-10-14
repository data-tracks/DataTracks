use std::collections::VecDeque;
use std::time::Instant;

use chrono::{Duration, NaiveTime};

use crate::processing::transform::Taker;
use crate::processing::window::Window::{Back, Interval, Non};
use crate::processing::Train;
use crate::util::TimeUnit;

#[derive(Clone)]
pub enum Window {
    Non(NonWindow),
    Back(BackWindow),
    Interval(IntervalWindow),
}


impl Default for Window {
    fn default() -> Self {
        Non(NonWindow::default())
    }
}


impl Window {
    pub(crate) fn windowing(&self) -> Box<dyn Taker> {
        match self {
            Back(w) => Box::new(w.clone()),
            Interval(w) => Box::new(w.clone()),
            Non(_) => Box::<NonWindow>::default()
        }
    }

    pub(crate) fn dump(&self) -> String {
        match self {
            Back(w) => w.dump(),
            Interval(w) => w.dump(),
            Non(_) => "".to_owned()
        }
    }

    pub(crate) fn parse(stencil: String) -> Self {
        if stencil.contains('@') {
            return Interval(IntervalWindow::parse(stencil));
        }
        Back(BackWindow::parse(stencil))
    }
}

#[derive(Clone, Default)]
pub struct NonWindow {}

impl NonWindow {}

impl Taker for NonWindow {
    fn take(&mut self, wagons: &mut Vec<Train>) -> Vec<Train> {
        wagons.clone()
    }
}


#[derive(Clone)]
pub struct BackWindow {
    duration: Duration,
    time: i64,
    time_unit: TimeUnit,
    buffer: VecDeque<(Instant, Vec<Train>)>,
}

impl BackWindow {
    pub fn new(time: i64, time_unit: TimeUnit) -> Self {
        BackWindow { time, time_unit: time_unit.clone(), duration: get_duration(time, time_unit), buffer: VecDeque::new() }
    }
    fn parse(stencil: String) -> Self {
        let (digit, time_unit) = parse_interval(stencil.as_str());

        BackWindow::new(digit, time_unit)
    }


    pub(crate) fn dump(&self) -> String {
        if self.time == 0 {
            return "".to_string();
        }
        format!("[{}{}]", &self.time, self.time_unit)
    }
}

impl Taker for BackWindow {
    fn take(&mut self, trains: &mut Vec<Train>) -> Vec<Train> {
        let instant = Instant::now();
        self.buffer.push_back((instant, trains.clone()));

        let mut values = vec![];
        let mut new_buffer = VecDeque::new();
        for (i, value) in self.buffer.clone() {
            if instant.checked_duration_since(i).unwrap().as_millis() <= self.duration.num_milliseconds() as u128 {
                values.append(value.clone().as_mut());
                new_buffer.push_back((i, value))
            }
        }
        self.buffer = new_buffer;

        values
    }
}

fn get_duration(time: i64, time_unit: TimeUnit) -> Duration {
    match time_unit {
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
    buffer: VecDeque<Vec<Train>>,
}

impl IntervalWindow {
    fn new(time: i64, time_unit: TimeUnit, start: NaiveTime) -> IntervalWindow {
        IntervalWindow { time, time_unit, start, buffer: VecDeque::new() }
    }
    pub(crate) fn dump(&self) -> String {
        format!("[{}{}@{}]", &self.time, self.time_unit, &self.start.format("%H:%M"))
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
}

impl Taker for IntervalWindow {
    fn take(&mut self, wagons: &mut Vec<Train>) -> Vec<Train> {
        self.buffer.push_back(wagons.clone());
        wagons.clone()
    }
}

fn parse_time(time_str: &str) -> Result<NaiveTime, chrono::ParseError> {
    NaiveTime::parse_from_str(time_str, "%H:%M")
}


#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use crossbeam::channel::unbounded;
    use crate::processing::Plan;
    use crate::processing::station::Command::Ready;
    use crate::processing::station::Station;
    use crate::processing::tests::dict_values;
    use crate::processing::train::Train;
    use crate::processing::window::{BackWindow, Window};
    use crate::util::{new_channel, TimeUnit};
    use crate::value::Value;

    #[test]
    fn default_behavior() {
        let mut station = Station::new(0);

        let control = unbounded();

        let values = dict_values(vec![Value::float(3.3), Value::int(3)]);

        let (tx, _num, rx) = new_channel();


        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0));
        station.send(Train::new(0, values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(mut t) => {
                assert_eq!(values.len(), t.values.clone().map_or(usize::MAX, |vec| vec.len()));
                for (i, value) in t.values.take().unwrap().into_iter().enumerate() {
                    assert_eq!(value, values[i]);
                    assert_ne!(Value::text(""), *value.as_dict().unwrap().get("$").unwrap())
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

        let values = dict_values(vec![Value::float(3.3), Value::int(3)]);
        let after = dict_values(vec!["test".into()]);

        let (tx, _num, rx) = new_channel();


        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0));
        // wait for read
        assert_eq!(Ready(0), control.1.recv().unwrap());

        for value in &values {
            station.send(Train::new(0, vec![value.clone()])).unwrap();
        }
        sleep(Duration::from_millis(20));

        let mut results = vec![];
        station.send(Train::new(0, after.clone())).unwrap();

        for _ in 0..3 {
            results.push(rx.recv().unwrap())
        }

        // 1. train
        assert_eq!(results.remove(0).values.take().unwrap().get(0).unwrap(), values.get(0).unwrap());
        // 2. "
        assert_eq!(results.remove(0).values.take().unwrap(), values);
        // 3. "
        assert_eq!(results.remove(0).values.take().unwrap(), after);
    }
}