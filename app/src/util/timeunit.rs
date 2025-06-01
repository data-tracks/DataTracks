use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, PartialEq)]
pub enum TimeUnit {
    Millis,
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl TimeUnit {
    pub(crate) fn dump_full(&self, _quote: &str) -> String {
        match self {
            TimeUnit::Millis => "MILLISECONDS".to_string(),
            TimeUnit::Seconds => "SECONDS".to_string(),
            TimeUnit::Minutes => "MINUTES".to_string(),
            TimeUnit::Hours => "HOURS".to_string(),
            TimeUnit::Days => "DAYS".to_string()
        }
    }

    pub fn as_ms(&self) -> i64 {
        match self {
            TimeUnit::Millis => 1,
            TimeUnit::Seconds => 1000,
            TimeUnit::Minutes => 60 * 1000,
            TimeUnit::Hours => 60 * 60 * 1000,
            TimeUnit::Days => 24 * 60 * 60 * 1000,
        }
    }
}

impl Display for TimeUnit{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.into())?;
        Ok(())
    }
}

const TIME_UNIT_MS: &str = "ms";

const TIME_UNIT_S: &str = "s";

const TIME_UNIT_M: &str = "m";

const TIME_UNIT_H: &str = "h";

const TIME_UNIT_D: &str = "d";

impl From<&TimeUnit> for &str {

    fn from(value: &TimeUnit) -> Self {
        match value {
            TimeUnit::Millis => TIME_UNIT_MS,
            TimeUnit::Seconds => TIME_UNIT_S,
            TimeUnit::Minutes => TIME_UNIT_M,
            TimeUnit::Hours => TIME_UNIT_H,
            TimeUnit::Days => TIME_UNIT_D
        }
    }
}

impl TryFrom<&str> for TimeUnit {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            TIME_UNIT_MS => Ok(TimeUnit::Millis),
            TIME_UNIT_S => Ok(TimeUnit::Seconds),
            TIME_UNIT_M => Ok(TimeUnit::Minutes),
            TIME_UNIT_H => Ok(TimeUnit::Hours),
            TIME_UNIT_D => Ok(TimeUnit::Days),
            _ => Err("Could not parse TimeUnit".to_string())
        }
    }
}

