use crate::value::{Bool, Float, Text};
use crate::value_display;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::ops::{Add, Div, Sub};
use speedy::{Readable, Writable};

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug, Serialize, Deserialize, Ord, PartialOrd, Readable, Writable)]
pub struct Int(pub(crate) i64);

impl Int{
    pub fn new(value: i64) -> Int{
        Int(value)
    }
}

impl Add for Int {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Int(self.0 + other.0)
    }
}

impl Sub for Int {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Int(self.0 - other.0)
    }
}

impl Div for Int {
    type Output = Self;

    fn div(self, other: Self) -> Self {
        Int(self.0 / other.0)
    }
}


// Adding IntWrapper to FloatWrapper
impl Add<Float> for Int {
    type Output = Float;

    fn add(self, other: Float) -> Float {
        Float::new(self.0 as f64 + other.as_f64())
    }
}

// Subtracting FloatWrapper from IntWrapper
impl Sub<Float> for Int {
    type Output = Float;

    fn sub(self, other: Float) -> Float {
        Float::new(self.0 as f64 - other.as_f64())
    }
}

impl Add<Bool> for Int {
    type Output = Int;

    fn add(self, rhs: Bool) -> Self::Output {
        Int(self.0 + if rhs.0 { 1 } else { 0 })
    }
}

impl Add<Box<Text>> for Int {
    type Output = Int;

    fn add(self, _: Box<Text>) -> Self::Output {
        panic!("Cannot add string to int")
    }
}

impl PartialEq<Float> for Int {
    fn eq(&self, other: &Float) -> bool {
        self.0 == other.as_f64() as i64
    }
}

impl PartialEq<Bool> for Int {
    fn eq(&self, other: &Bool) -> bool {
        self.0 > 0 && other.0
    }
}

impl PartialEq<Text> for Int {
    fn eq(&self, other: &Text) -> bool {
        match other.0.parse::<i64>() {
            Ok(i) => i == self.0,
            Err(_) => false
        }
    }
}

value_display!(Int);

#[cfg(test)]
mod tests {
    use crate::value::Int;

    #[test]
    fn add() {
        let int = Int(35);

        let res = int + int;

        assert_eq!(res.0, 35 + 35)
    }
}
