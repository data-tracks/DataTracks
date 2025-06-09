use chrono::{Duration, NaiveDate};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::fmt::Formatter;

const EPOCH_DATE: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

#[derive(
    Clone, Debug, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize, Readable, Writable,
)]
pub struct Date {
    pub days: i64,
}

impl Date {
    pub fn new(days: i64) -> Date {
        Date { days }
    }

    pub fn as_epoch(&self) -> i64 {
        self.days
    }
}

pub struct TimeContainer {
    pub(crate) year: i32,
    pub(crate) month: u32,
    pub(crate) day: u32,
}

impl std::fmt::Display for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let target_date = EPOCH_DATE + Duration::days(self.days);
        write!(f, "{}", target_date.format("%Y-%m-%d"))
    }
}

impl From<TimeContainer> for Date {
    fn from(time: TimeContainer) -> Self {
        let date = NaiveDate::from_ymd_opt(time.year, time.month, time.day).unwrap();

        Date::new((date - EPOCH_DATE).num_days())
    }
}

impl From<String> for Date {
    fn from(string: String) -> Self {
        Self::from(string.as_str())
    }
}

impl From<&str> for Date {
    fn from(string: &str) -> Self {
        let date = NaiveDate::parse_from_str(string, "%Y-%m-%d").unwrap();

        Date::new((date - EPOCH_DATE).num_days())
    }
}
