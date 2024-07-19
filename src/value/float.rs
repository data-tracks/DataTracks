use std::fmt::Formatter;
use std::ops::{Add, Sub};

use crate::value::{HoBool, HoString};
use crate::value::int::HoInt;
use crate::value::number::Number;
use crate::value::value::{ValType, Valuable, value_display};
use crate::value::value::ValType::Float;

#[derive(Eq, Hash, Debug, PartialEq, Clone, Copy)]
pub struct HoFloat(pub i64, pub u64);

impl HoFloat {
    pub(crate) fn as_f64(&self) -> f64 {
        self.0 as f64 + self.1 as f64 * 10f64.powi(-(self.1.to_string().len() as i32))
    }

    pub(crate) fn new(value: f64) -> Self {
        let parsed = value.to_string();
        let split = parsed.split_once('.');
        match split {
            None => HoFloat(value as i64, 0),
            Some((a, b)) => HoFloat(a.parse().unwrap(), b.parse().unwrap())
        }
    }
}


impl Valuable for HoFloat {
    fn type_(&self) -> ValType {
        Float
    }
}

impl Number for HoFloat {
    fn float(&self) -> f64 {
        self.as_f64()
    }

    fn int(&self) -> i64 {
        self.0
    }
}

impl Add for HoFloat {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        HoFloat::new(self.as_f64() + other.as_f64())
    }
}

impl Sub for HoFloat {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        HoFloat::new(self.as_f64() - other.as_f64())
    }
}


// Adding FloatWrapper to IntWrapper
impl Add<HoInt> for HoFloat {
    type Output = HoFloat;

    fn add(self, other: HoInt) -> HoFloat {
        HoFloat(self.0 + other.0, self.1)
    }
}

// Subtracting IntWrapper from FloatWrapper
impl Sub<HoInt> for HoFloat {
    type Output = HoFloat;

    fn sub(self, other: HoInt) -> HoFloat {
        HoFloat(self.0 - other.0, self.1)
    }
}

impl PartialEq<HoInt> for HoFloat {
    fn eq(&self, other: &HoInt) -> bool {
        other == self
    }
}

impl PartialEq<HoBool> for HoFloat {
    fn eq(&self, other: &HoBool) -> bool {
        self.0 > 0 && other.0
    }
}

impl PartialEq<Box<HoString>> for HoFloat {
    fn eq(&self, other: &Box<HoString>) -> bool {
        match other.0.parse::<f64>() {
            Ok(f) => f == self.as_f64(),
            Err(_) => false
        }
    }
}

value_display!(HoFloat);

#[cfg(test)]
mod tests {
    use crate::value::HoFloat;

    #[test]
    fn add() {
        let float = HoFloat::new(35.5);

        let res = float + float;

        assert_eq!(res.as_f64(), 35.5 + 35.5)
    }
}

