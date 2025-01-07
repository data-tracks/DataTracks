use crate::value::{Text, Value};
use chrono::{DateTime, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use speedy::{Readable, Writable};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Readable, Writable)]
pub struct Time {
    pub ns: u32,
    pub ms: usize,
}

impl Time {
    pub fn new(ms: usize, ns: u32) -> Time {
        if ns >= 1000000 {
            let ms = ms + (ns/1000000) as usize;
            let ns = ns % 1000000;
            return Time { ns, ms };
        }
        Time { ms, ns }
    }

    pub fn now() -> Time {
        let now_utc = Utc::now();

        let ms = now_utc.timestamp_millis();
        let ns = now_utc.timestamp_nanos_opt().unwrap_or_default();
        Value::time(ms as usize, ns as u32).as_time().unwrap()
    }
}

impl From<&Instant> for Time {
    fn from(instant: &Instant) -> Self {
        let now_system_time = SystemTime::now();
        let now_instant = Instant::now();

        // Calculate the duration between the Instant and the SystemTime
        let duration_since_epoch = now_system_time
            .duration_since(UNIX_EPOCH)
            .expect("System time is before UNIX epoch")
            - now_instant.duration_since(*instant);

        // Extract seconds and nanoseconds
        let total_millis = duration_since_epoch.as_millis() as usize;
        let nanos = duration_since_epoch.subsec_nanos();

        Time::new(total_millis, nanos)
    }
}

impl From<Text> for Time {
    fn from(value: Text) -> Self {
        let datetime: DateTime<Utc> = value.0.parse().unwrap();
        Time::from(datetime)
    }
}

impl<T: TimeZone> From<DateTime<T>> for Time {
    fn from(value: DateTime<T>) -> Self {
        let ns = value.time().nanosecond();
        let ms = value.timestamp_millis() as usize;
        Time::new(ms, ns)
    }
}

impl From<Time> for Value {
    fn from(time: Time) -> Self {
        Value::Time(time)
    }
}

impl std::fmt::Display for Time {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ns = self.ns;
        let hours = self.ms / 3600;
        let minutes = self.ms % 3600;
        let seconds = self.ms % 60;
        let mut string = format!("{:02}:{:02}:{:02}", hours, minutes, seconds);
        if ns > 0 {
            string.push_str(format!(".{:03}", ns).as_str());
        }

        write!(f, "{}", string)
    }
}
