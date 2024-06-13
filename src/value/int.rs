use std::fmt::Formatter;
use std::ops::{Add, Sub};

use crate::value::{HoBool, HoFloat, HoString};
use crate::value::number::Number;
use crate::value::value::{ValType, Valuable, value_display};
use crate::value::value::ValType::Integer;

#[derive(PartialEq, Clone, Copy, Debug)]
pub struct HoInt(pub(crate) i64);

impl Valuable for HoInt {
    fn type_(&self) -> ValType {
        Integer
    }
}

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

impl Add<HoBool> for HoInt {
    type Output = HoInt;

    fn add(self, rhs: HoBool) -> Self::Output {
        HoInt(self.0 + if rhs.0 { 1 } else { 0 })
    }
}

impl Add<Box<HoString>> for HoInt {
    type Output = HoInt;

    fn add(self, _: Box<HoString>) -> Self::Output {
        panic!("Cannot add string to int")
    }
}

impl PartialEq<HoFloat> for HoInt {
    fn eq(&self, other: &HoFloat) -> bool {
        self.0 as f64 == other.0
    }
}

impl PartialEq<HoBool> for HoInt {
    fn eq(&self, other: &HoBool) -> bool {
        self.0 > 0 && other.0
    }
}

impl PartialEq<Box<HoString>> for HoInt {
    fn eq(&self, other: &Box<HoString>) -> bool {
        match other.0.parse::<i64>() {
            Ok(i) => i == self.0,
            Err(_) => false
        }
    }
}



value_display!(HoInt);

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
