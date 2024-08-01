use std::fmt::Formatter;
use std::ops::{Add, Sub};

use crate::value::{Bool, Text};
use crate::value::int::Int;
use crate::value::number::Number;
use crate::value_display;

#[derive(Eq, Hash, Debug, PartialEq, Clone, Copy)]
pub struct Float(pub i64, pub u64);

impl Float {
    pub(crate) fn as_f64(&self) -> f64 {
        self.0 as f64 + self.1 as f64 * 10f64.powi(-(self.1.to_string().len() as i32))
    }

    pub(crate) fn new(value: f64) -> Self {
        let parsed = value.to_string();
        let split = parsed.split_once('.');
        match split {
            None => Float(value as i64, 0),
            Some((a, b)) => Float(a.parse().unwrap(), b.parse().unwrap())
        }
    }
}


impl Number for Float {
    fn float(&self) -> f64 {
        self.as_f64()
    }

    fn int(&self) -> i64 {
        self.0
    }
}

impl Add for Float {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Float::new(self.as_f64() + other.as_f64())
    }
}

impl Sub for Float {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Float::new(self.as_f64() - other.as_f64())
    }
}


// Adding FloatWrapper to IntWrapper
impl Add<Int> for Float {
    type Output = Float;

    fn add(self, other: Int) -> Float {
        Float(self.0 + other.0, self.1)
    }
}

// Subtracting IntWrapper from FloatWrapper
impl Sub<Int> for Float {
    type Output = Float;

    fn sub(self, other: Int) -> Float {
        Float(self.0 - other.0, self.1)
    }
}

impl PartialEq<Int> for Float {
    fn eq(&self, other: &Int) -> bool {
        other == self
    }
}

impl PartialEq<Bool> for Float {
    fn eq(&self, other: &Bool) -> bool {
        self.0 > 0 && other.0
    }
}

impl PartialEq<Text> for Float {
    fn eq(&self, other: &Text) -> bool {
        match other.0.parse::<f64>() {
            Ok(f) => f == self.as_f64(),
            Err(_) => false
        }
    }
}

value_display!(Float);

#[cfg(test)]
mod tests {
    use crate::value::Float;

    #[test]
    fn add() {
        let float = Float::new(35.5);

        let res = float + float;

        assert_eq!(res.as_f64(), 35.5 + 35.5)
    }
}

