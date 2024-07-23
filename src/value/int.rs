use std::fmt::Formatter;
use std::ops::{Add, Sub};

use crate::value::{Bool, Float, Text, ValType};
use crate::value::number::Number;
use crate::value::ValType::Integer;
use crate::value::value::{Valuable};
use crate::value_display;

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug)]
pub struct Int(pub(crate) i64);

impl Valuable for Int {
    fn type_(&self) -> ValType {
        Integer
    }
}

impl Number for Int {
    fn float(&self) -> f64 {
        self.0 as f64
    }
    fn int(&self) -> i64 {
        self.0
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


// Adding IntWrapper to FloatWrapper
impl Add<Float> for Int {
    type Output = Float;

    fn add(self, other: Float) -> Float {
        Float(self.0 + other.0, other.1)
    }
}

// Subtracting FloatWrapper from IntWrapper
impl Sub<Float> for Int {
    type Output = Float;

    fn sub(self, other: Float) -> Float {
        Float(self.0 - other.0, other.1)
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
        self.0 == other.0
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
