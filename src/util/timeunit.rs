use std::fmt::{Display, Formatter};

#[derive( Clone)]
pub enum TimeUnit {
    Millis,
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl Display for TimeUnit{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.into()).unwrap();
        Ok(())
    }
}

const TIME_UNIT_MS: &'static str = "ms";

const TIME_UNIT_S: &'static str = "s";

const TIME_UNIT_M: &'static str = "m";

const TIME_UNIT_H: &'static str = "h";

const TIME_UNIT_D: &'static str = "d";

impl Into<&str> for &TimeUnit {
    fn into(self) -> &'static str {
        match self {
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

