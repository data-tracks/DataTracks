use crate::processing::select::WindowDescriptor;
use crate::processing::window::Window::{Back, Interval, Non};
use crate::processing::Train;
use crate::util::TimeUnit;
use chrono::{Duration, NaiveTime, Timelike};
use value::Time;

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
    pub(crate) fn get_strategy(&self) -> WindowStrategy {
        match self {
            Non(_) => WindowStrategy::None(NoneStrategy::new()),
            Back(b) => WindowStrategy::Back(BackStrategy::new(b)),
            Interval(i) => WindowStrategy::Interval(IntervalStrategy::new(i)),
        }
    }

    pub(crate) fn dump(&self) -> String {
        match self {
            Back(w) => w.dump(),
            Interval(w) => w.dump(),
            Non(_) => "".to_owned(),
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

#[derive(Clone)]
pub struct BackWindow {
    pub duration: Duration,
    time: i64,
    time_unit: TimeUnit,
}

impl BackWindow {
    pub fn new(time: i64, time_unit: TimeUnit) -> Self {
        BackWindow {
            time,
            time_unit: time_unit.clone(),
            duration: get_duration(time, time_unit),
        }
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

fn get_duration(time: i64, time_unit: TimeUnit) -> Duration {
    match time_unit {
        TimeUnit::Millis => Duration::milliseconds(time),
        TimeUnit::Seconds => Duration::seconds(time),
        TimeUnit::Minutes => Duration::minutes(time),
        TimeUnit::Hours => Duration::hours(time),
        TimeUnit::Days => Duration::days(time),
    }
}

fn parse_interval(stencil: &str) -> Result<(i64, TimeUnit), String> {
    let mut temp = "".to_string();
    let mut digit: i64 = 0;
    let mut digit_passed: bool = false;
    for char in stencil.chars() {
        if !char.is_numeric() && !digit_passed {
            digit = temp
                .parse()
                .map_err(|_| format!("Could not parse {} as time", stencil))?;
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
    pub time: i64,
    pub time_unit: TimeUnit,
    pub start: Time,
    pub millis_delta: i64,
}

impl IntervalWindow {
    fn new(time: i64, time_unit: TimeUnit, start: Time) -> IntervalWindow {
        IntervalWindow {
            time,
            time_unit: time_unit.clone(),
            start,
            millis_delta: time * time_unit.as_ms(),
        }
    }

    pub(crate) fn dump(&self) -> String {
        format!(
            "[{}{}@{}]",
            &self.time,
            self.time_unit,
            &self.start.to_string()
        )
    }

    pub(crate) fn get_times(&self, time: &Time) -> (Time, Time) {
        let mut temp = self.start;

        while &temp < time {
            temp += self.millis_delta;
        }
        (temp, temp + self.millis_delta)
    }

    fn parse(input: String) -> Result<IntervalWindow, String> {
        match input.split_once('@') {
            None => {
                let (time, time_unit) = parse_interval(&input)?;
                let start = Time::new(0, 0);

                Ok(IntervalWindow::new(time, time_unit, start))
            }
            Some((interval, start)) => {
                let (time, time_unit) = parse_interval(interval)?;
                let start = parse_time(start)?;

                Ok(IntervalWindow::new(time, time_unit, start))
            }
        }
    }
}

fn parse_time(time_str: &str) -> Result<Time, String> {
    let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S").unwrap_or_else(|v| {
        NaiveTime::parse_from_str(time_str, "%H:%M")
            .unwrap_or_else(|_| NaiveTime::parse_from_str(time_str, "%H:%M:%s:6f").unwrap())
    });
    println!("time {time_str}");
    Ok(Time::new(
        (time.num_seconds_from_midnight() * 1000) as i64,
        0,
    ))
}

pub enum WindowStrategy {
    None(NoneStrategy),
    Back(BackStrategy),
    Interval(IntervalStrategy),
}

impl WindowStrategy {
    pub(crate) fn mark(&mut self, train: &Train) -> Vec<(WindowDescriptor, bool)> {
        match self {
            WindowStrategy::None(n) => n.mark(train),
            WindowStrategy::Back(b) => b.mark(train),
            WindowStrategy::Interval(i) => i.mark(train),
        }
    }
}

pub struct NoneStrategy {}

impl NoneStrategy {
    pub(crate) fn new() -> Self {
        NoneStrategy {}
    }
    pub(crate) fn mark(&mut self, train: &Train) -> Vec<(WindowDescriptor, bool)> {
        vec![(WindowDescriptor::unbounded(train.event_time.clone()), true)]
    }
}

pub struct BackStrategy {
    pub duration: Duration,
    time: i64,
    time_unit: TimeUnit,
    delta_ms: i64,
}

impl BackStrategy {
    pub(crate) fn new(window: &BackWindow) -> Self {
        Self {
            duration: window.duration,
            time: window.time,
            time_unit: window.time_unit.clone(),
            delta_ms: window.time * window.time_unit.as_ms(),
        }
    }

    pub(crate) fn mark(&self, train: &Train) -> Vec<(WindowDescriptor, bool)> {
        let start = Time::new(train.event_time.ms - self.delta_ms, train.event_time.ns);
        vec![(WindowDescriptor::new(start, train.event_time), true)]
    }
}

pub struct IntervalStrategy {
    pub start: Time,
    pub millis_delta: i64,
}

impl IntervalStrategy {
    pub(crate) fn mark(&self, train: &Train) -> Vec<(WindowDescriptor, bool)> {
        let elapsed = train.event_time.duration_since(self.start);
        let start_delta = elapsed.ms % self.millis_delta;
        let start = train.event_time.ms - start_delta;
        vec![(
            WindowDescriptor::new(
                Time::new(start, self.start.ns),
                Time::new(start + self.millis_delta, self.start.ns),
            ),
            false,
        )]
    }
}

impl IntervalStrategy {
    fn new(window: &IntervalWindow) -> Self {
        Self {
            start: window.start,
            millis_delta: window.millis_delta,
        }
    }
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
    use crate::processing::window::{BackWindow, Window};
    use crate::util::{new_channel, TimeUnit};
    use crossbeam::channel::unbounded;
    use value::train::Train;
    use value::Value;

    #[test]
    fn default_behavior() {
        let mut station = Station::new(0);

        let control = unbounded();

        let values = dict_values(vec![Value::float(3.3), Value::int(3)]);

        let (tx, rx) = new_channel("test", false);

        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0), HashMap::new());
        station.fake_receive(Train::new(values.clone()));

        let res = rx.recv();
        match res {
            Ok(t) => {
                assert_eq!(values.len(), t.values.len());
                for (i, value) in t.values.into_iter().enumerate() {
                    assert_eq!(value, values[i]);
                    assert_ne!(Value::text(""), *value.as_dict().unwrap().get("$").unwrap())
                }
            }
            Err(..) => panic!(),
        }
    }

    #[test]
    fn back_behavior() {
        let mut station = Station::new(0);

        station.window = Window::Back(BackWindow::new(5, TimeUnit::Millis));

        let control = unbounded();

        let values = dict_values(vec![Value::float(3.3), Value::int(3)]);
        let after = dict_values(vec!["test".into()]);

        let (tx, rx) = new_channel("test", false);

        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0), HashMap::new());
        // wait for read
        assert_eq!(Ready(0), control.1.recv().unwrap());

        for value in &values {
            station.fake_receive(Train::new(vec![value.clone()]));
        }
        sleep(Duration::from_millis(50));

        let mut results = vec![];
        // receive first 2
        for _ in 0..2 {
            results.push(rx.recv().unwrap())
        }

        station.fake_receive(Train::new(after.clone()));

        //receive last
        results.push(rx.recv().unwrap());

        // 1. train
        assert_eq!(
            results.remove(0).values.get(0).unwrap(),
            values.get(0).unwrap()
        );
        // 2. " or 1. & 2. depending on how fast it was handled
        let res = results.remove(0).values;

        if res.len() == 1 {
            assert_eq!(res.get(0).unwrap(), values.get(0).unwrap());
        } else {
            assert!(
                res.get(0).unwrap() == values.get(1).unwrap()
                    || res.get(1).unwrap() == values.get(1).unwrap()
                    || res.get(0).unwrap() == values.get(1).unwrap()
            )
        }

        // 3. "
        assert_eq!(results.remove(0).values, after);
    }
}
