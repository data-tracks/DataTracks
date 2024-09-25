use crate::value::int::Int;
use crate::value::number::Number;
use crate::value::{Bool, Text};
use std::fmt::{Display, Formatter};
use std::ops::{Add, Sub};

#[derive(Eq, Hash, Debug, PartialEq, Clone, Copy)]
pub struct Float {
    pub number: i64,
    pub shift: u8,
}

impl Float {
    pub(crate) fn as_f64(&self) -> f64 {
        self.number as f64 / 10_f64.powi(self.shift as i32)
    }

    pub(crate) fn new(value: f64) -> Self {
        let parsed = value.to_string();
        let number = parsed.replace('.', "");
        let split = parsed.find('.');

        match split {
            None => Float { number: value as i64, shift: 0 },
            Some(i) => Float { number: number.parse().unwrap(), shift: (number.len() - i) as u8 }
        }
    }
}


impl Number for Float {
    fn float(&self) -> f64 {
        self.as_f64()
    }

    fn int(&self) -> i64 {
        self.number
    }
}

impl Add for Float {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let mut shift = self.shift as i64 - other.shift as i64;
        let mut left = self.number;
        let mut right = other.number;

        match shift {
            s if s > 0 => {
                // self is bigger fract
                right *= shift * 10;
                shift = self.shift as i64;
            }
            s if s < 0 => {
                // other is bigger fract
                left *= shift * -10;
                shift = other.shift as i64;
            }
            _ => shift = self.shift as i64
        }

        Float { number: left + right, shift: shift as u8 }
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
        Float { number: self.number + other.0, shift: self.shift }
    }
}

// Subtracting IntWrapper from FloatWrapper
impl Sub<Int> for Float {
    type Output = Float;

    fn sub(self, other: Int) -> Float {
        Float { number: self.number - other.0 * (10 * self.shift) as i64, shift: self.shift }
    }
}

impl PartialEq<Int> for Float {
    fn eq(&self, other: &Int) -> bool {
        other == self
    }
}

impl PartialEq<Bool> for Float {
    fn eq(&self, other: &Bool) -> bool {
        self.number > 0 && other.0
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


impl Display for Float {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_f64())
    }
}


#[cfg(test)]
mod tests {
    use crate::value::Float;

    #[test]
    fn serialize_deserialize() {
        let float = Float::new(35.5);

        let res = float.as_f64();

        assert_eq!(res, 35.5)
    }

    #[test]
    fn add() {
        let float = Float::new(35.5);

        let res = float + float;

        assert_eq!(res.as_f64(), 35.5 + 35.5)
    }
}

