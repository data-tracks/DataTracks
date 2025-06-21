use crate::util::TimeUnit;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub enum TriggerType {
    Element,
    Interval(isize, TimeUnit),
    WindowEnd,
    WindowNext,
}

impl Display for TriggerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.dump(""))
    }
}

impl TriggerType {
    pub fn dump(&self, quote: &str) -> String {
        match self {
            TriggerType::Element => String::from("ELEMENT"),
            TriggerType::Interval(amount, unit) => {
                format!("INTERVAL({} {})", amount, unit.dump_full(quote))
            }
            TriggerType::WindowEnd => String::from("WINDOW END"),
            TriggerType::WindowNext => String::from("WINDOW NEXT"),
        }
    }
}
