use crate::int::Int;
use crate::{Bool, Text};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::fmt::{Display, Formatter};
use std::ops::{Add, Sub};

#[derive(
    Eq,
    Hash,
    Debug,
    PartialEq,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    Ord,
    PartialOrd,
    Readable,
    Writable,
)]
pub struct Float (pub OrderedFloat<f64>);

impl From<f64> for Float {
    fn from(x: f64) -> Float {
        Float(OrderedFloat(x))
    }
}

// Adding FloatWrapper to IntWrapper
impl Add<Int> for Float {
    type Output = Float;

    fn add(self, other: Int) -> Float {
        Float(OrderedFloat(self.0.0 + other.0 as f64))
    }
}

impl Add<Float> for Float {
    type Output = Float;

    fn add(self, other: Float) -> Float {
        Float(OrderedFloat(self.0.0 + other.0.0))
    }
}

// Subtracting IntWrapper from FloatWrapper
impl Sub<Int> for Float {
    type Output = Float;

    fn sub(self, other: Int) -> Float {
        Float(OrderedFloat(self.0.0 - other.0 as f64))
    }
}

impl Sub<Float> for Float {
    type Output = Float;

    fn sub(self, other: Float) -> Float {
        Float(OrderedFloat(self.0.0 - other.0.0))
    }
}

impl PartialEq<Int> for Float {
    fn eq(&self, other: &Int) -> bool {
        other == self
    }
}

impl PartialEq<Bool> for Float {
    fn eq(&self, other: &Bool) -> bool {
        self.0.0 > 0f64 && other.0
    }
}

impl PartialEq<Text> for Float {
    fn eq(&self, other: &Text) -> bool {
        match other.0.parse::<f64>() {
            Ok(f) => f == self.0.0,
            Err(_) => false,
        }
    }
}

impl Display for Float {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::Float;


    #[test]
    fn add() {
        let float:Float = 35.5.into();

        let res = float + float;

        assert_eq!(res.0, 35.5 + 35.5)
    }
}
