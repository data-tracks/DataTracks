use std::fmt;
use std::fmt::Formatter;
use std::ops::{Add, Sub};
use crate::value::HoFloat;
use crate::value::number::Number;


#[derive(Debug, PartialEq, Clone, Copy)]
pub struct HoInt(pub(crate) i64);

impl Number for HoInt {
    fn float(&self) -> f64 {
        self.0 as f64
    }
    fn int(&self) -> i64 {
        self.0
    }
}


impl Add for HoInt {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        HoInt(self.0 + other.0)
    }
}

impl Sub for HoInt {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        HoInt(self.0 - other.0)
    }
}


// Adding IntWrapper to FloatWrapper
impl Add<HoFloat> for HoInt {
    type Output = HoFloat;

    fn add(self, other: HoFloat) -> HoFloat {
        HoFloat(self.0 as f64 + other.0)
    }
}

// Subtracting FloatWrapper from IntWrapper
impl Sub<HoFloat> for HoInt {
    type Output = HoFloat;

    fn sub(self, other: HoFloat) -> HoFloat {
        HoFloat(self.0 as f64 - other.0)
    }
}

impl fmt::Display for HoInt {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::value::HoInt;

    #[test]
    fn add() {
        let int = HoInt(35);

        let res = int + int;

        assert_eq!(res.0, 35 + 35)
    }
}
