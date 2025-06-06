use crate::{Text, Value};
use chrono::{DateTime, Duration, TimeZone, Timelike, Utc};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use schemas::message_generated::protocol::{Time as FlatTime, TimeArgs};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::ops;
use std::ops::Sub;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Readable, Writable, Copy)]
pub struct Time {
    pub ms: i64,
    pub ns: u32,
}

impl Default for Time {
    fn default() -> Self {
        Time::now()
    }
}

impl Time {
    pub fn new(ms: i64, ns: u32) -> Time {
        if ns >= 1000000 {
            let ms = ms + (ns / 1000000) as i64;
            let ns = ns % 1000000;
            return Time { ns, ms };
        }
        Time { ms, ns }
    }

    pub fn duration_since(&self, other: Time) -> Time {
        Time::new(other.ms - self.ms, other.ns - self.ns)
    }

    pub(crate) fn flatternize<'bldr>(
        &self,
        builder: &mut FlatBufferBuilder<'bldr>,
    ) -> WIPOffset<FlatTime<'bldr>> {
        FlatTime::create(
            builder,
            &TimeArgs {
                data: self.ms as i64,
            },
        )
    }

    pub fn now() -> Time {
        let now_utc = Utc::now();

        let ms = now_utc.timestamp_millis();
        let ns = now_utc.timestamp_nanos_opt().unwrap_or_default();
        Value::time(ms, ns as u32).as_time().unwrap()
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
        let total_millis = duration_since_epoch.as_millis() as i64;
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
        let ms = value.timestamp_millis();
        Time::new(ms, ns)
    }
}

impl From<Time> for Value {
    fn from(time: Time) -> Self {
        Value::Time(time)
    }
}

impl Sub<Duration> for &Time {
    type Output = Time;

    fn sub(self, rhs: Duration) -> Self::Output {
        Value::time(
            self.ms - rhs.num_milliseconds(),
            rhs.num_nanoseconds()
                .map(|ns| self.ns as i64 - ns)
                .unwrap_or(0) as u32,
        )
        .as_time()
        .unwrap()
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

impl ops::AddAssign<i64> for Time {
    fn add_assign(&mut self, rhs: i64) {
        self.ms += rhs;
    }
}

impl ops::Add<i64> for Time {
    type Output = Time;

    fn add(mut self, rhs: i64) -> Self::Output {
        self.ms = rhs;
        self
    }
}

impl PartialOrd for Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.ms.partial_cmp(&other.ms) {
            None => None,
            Some(ord) => {
                if ord == Ordering::Equal {
                    self.ns.partial_cmp(&other.ns)
                } else {
                    Some(ord)
                }
            }
        }
    }
}

impl Ord for Time {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.ms.cmp(&other.ms) {
            Ordering::Equal => self.ns.cmp(&other.ns),
            ord => ord,
        }
    }
}
