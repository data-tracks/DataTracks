use std::fmt::Formatter;
use crate::value::Text;
use chrono::{DateTime, TimeZone, Timelike, Utc};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Time {
    pub ns: u32,
    pub ms: usize
}

impl Time {
    pub fn new(ms: usize, ns: u32) -> Time {
        Time {ms, ns}
    }

    pub fn to_string(&self) -> String {
        let ns = self.ns.clone();
        let hours = self.ms / 3600;
        let minutes = self.ms % 3600;
        let seconds = self.ms % 60;
        let mut string = format!("{:02}:{:02}:{:02}", hours, minutes, seconds);
        if ns > 0 {
            string.push_str(format!(".{:03}", ns).as_str());
        }
        string
    }

    pub fn now() -> String {
        let now_utc = Utc::now();
        now_utc.to_rfc3339() // ISO 8601 format
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
            - now_instant.duration_since(instant.clone());

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

impl<T:TimeZone> From<DateTime<T>> for Time {
    fn from(value: DateTime<T>) -> Self {
        let ns = value.time().nanosecond();
        let ms = value.timestamp_millis() as usize;
        Time::new(ms, ns)
    }
}

impl std::fmt::Display for Time {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}
