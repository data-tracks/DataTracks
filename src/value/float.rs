use std::fmt;
use std::fmt::Formatter;
use std::ops::{Add, Sub};

use crate::value::int::HoInt;
use crate::value::number::Number;

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct HoFloat(pub f64);


impl Number for HoFloat {
    fn float(&self) -> f64 {
        self.0
    }

    fn int(&self) -> i64 {
        self.0 as i64
    }
}

impl Add for HoFloat {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        HoFloat(self.0 + other.0)
    }
}

impl Sub for HoFloat {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        HoFloat(self.0 - other.0)
    }
}


// Adding FloatWrapper to IntWrapper
impl Add<HoInt> for HoFloat {
    type Output = HoFloat;

    fn add(self, other: HoInt) -> HoFloat {
        HoFloat(self.0 + other.0 as f64)
    }
}

// Subtracting IntWrapper from FloatWrapper
impl Sub<HoInt> for HoFloat {
    type Output = HoFloat;

    fn sub(self, other: HoInt) -> HoFloat {
        HoFloat(self.0 - other.0 as f64)
    }
}

impl fmt::Display for HoFloat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::value::HoFloat;

    #[test]
    fn add() {
        let float = HoFloat(35.5);

        let res = float + float;

        assert_eq!(res.0, 35.5 + 35.5)
    }
}

