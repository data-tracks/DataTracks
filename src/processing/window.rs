use crate::processing::transform::Taker;
use crate::processing::window::Window::{Back, Interval, Non};
use crate::processing::Train;
use crate::util;
use crate::util::{Storage, TimeUnit};
use crate::value::Time;
use chrono::{Duration, NaiveTime};
use std::collections::VecDeque;
use std::sync::Arc;
use speedy::{Readable, Writable};

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

    pub(crate) fn parse(stencil: String) -> Result<Self, String> {
        if stencil.contains('@') {
            return Ok(Interval(IntervalWindow::parse(stencil)?));
        }
        Ok(Back(BackWindow::parse(stencil)?))
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
    buffer: VecDeque<Time>,
    cache: Storage<Time, Vec<Train>>,
    storage: Arc<util::storage::Storage>
}

impl BackWindow {
    pub fn new(time: i64, time_unit: TimeUnit) -> Self {
        let now = Time::now();
        BackWindow { time, time_unit: time_unit.clone(), duration: get_duration(time, time_unit), buffer: VecDeque::new(), cache: Storage::new(100), storage: Arc::new(util::storage::Storage::new_from_path("DB.db".to_string(), now.ms.to_string()).unwrap()) }
    }
    fn parse(stencil: String) -> Result<Self, String> {
        let (digit, time_unit) = parse_interval(stencil.as_str())?;

        Ok(BackWindow::new(digit, time_unit))
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
        let time = Time::now();
        self.cache.set(time.clone(), trains.clone());
        self.storage.write(time.clone().into(), trains.write_to_vec().unwrap()).unwrap();
        let ms = time.ms;

        let mut values = vec![];

        for i in &self.buffer {
            if ms - i.ms <= self.duration.num_milliseconds() as usize {
                let value = if let Some(val) = self.cache.get(&i.clone()){
                    val.clone()
                }else {
                    Readable::read_from_buffer(&self.storage.read_u8(i.clone().into()).unwrap()).unwrap()
                };

                values.append(&mut value.clone());
            }
        }
        values.append(trains);
        self.buffer.push_back(time);


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

fn parse_interval(stencil: &str) -> Result<(i64, TimeUnit), String> {
    let mut temp = "".to_string();
    let mut digit: i64 = 0;
    let mut digit_passed: bool = false;
    for char in stencil.chars() {
        if !char.is_numeric() && !digit_passed {
            digit = temp.parse().map_err(|_| format!("Could not parse {} as time", stencil))?;
            digit_passed = false;
            temp = "".to_string();
        }
        temp.push(char);
    }
    let time_unit = parse_time_unit(temp)?;
    Ok((digit, time_unit))
}

fn parse_time_unit(time: String) -> Result<TimeUnit, String> {
    TimeUnit::try_from(time.as_str()).map_err(|e| e.to_string())
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
    fn parse(input: String) -> Result<IntervalWindow, String> {
        match input.split_once('@') {
            None => {
                let (time, time_unit) = parse_interval(&input)?;
                let start = NaiveTime::from_hms_opt(0, 0, 0).ok_or("Could not parse start time for interval".to_string())?;

                Ok(IntervalWindow::new(time, time_unit, start))
            },
            Some((interval, start)) => {
                let (time, time_unit) = parse_interval(interval)?;
                let start = parse_time(start).unwrap();

                Ok(IntervalWindow::new(time, time_unit, start))
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
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use crate::processing::station::Command::Ready;
    use crate::processing::station::Station;
    use crate::processing::tests::dict_values;
    use crate::processing::train::Train;
    use crate::processing::window::{BackWindow, Window};
    use crate::util::{new_channel, TimeUnit};
    use crate::value::Value;
    use crossbeam::channel::unbounded;

    #[test]
    fn default_behavior() {
        let mut station = Station::new(0);

        let control = unbounded();

        let values = dict_values(vec![Value::float(3.3), Value::int(3)]);

        let (tx, _num, rx) = new_channel();


        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0), HashMap::new());
        station.send(Train::new(values.clone())).unwrap();

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

        station.window = Window::Back(BackWindow::new(5, TimeUnit::Millis));

        let control = unbounded();

        let values = dict_values(vec![Value::float(3.3), Value::int(3)]);
        let after = dict_values(vec!["test".into()]);

        let (tx, _num, rx) = new_channel();


        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0), HashMap::new());
        // wait for read
        assert_eq!(Ready(0), control.1.recv().unwrap());

        for value in &values {
            station.send(Train::new(vec![value.clone()])).unwrap();
        }
        sleep(Duration::from_millis(50));

        let mut results = vec![];
        station.send(Train::new(after.clone())).unwrap();

        for _ in 0..3 {
            results.push(rx.recv().unwrap())
        }

        // 1. train
        assert_eq!(results.remove(0).values.take().unwrap().get(0).unwrap(), values.get(0).unwrap());
        // 2. " or 1. & 2. depending on how fast it was handled
        let res = results.remove(0).values.take().unwrap();
        assert!(res.get(0).unwrap() == values.get(1).unwrap() || res.get(1).unwrap() == values.get(1).unwrap() );

        // 3. "
        assert_eq!(results.remove(0).values.take().unwrap(), after);
    }
}