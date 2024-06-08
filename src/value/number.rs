
use std::fmt;
use std::fmt::Formatter;
use std::ops::{Add, Sub};
use crate::value::float::HoFloat;
use crate::value::int::HoInt;

pub trait Number: Add<Self, Output=Self> + Sub<Self, Output=Self> + Sized {
    fn float(&self) -> f64;
    fn int(&self) -> i64;
}



// Define an enum to hold either an Int or a Float
#[derive(Copy, Clone, Debug)]
pub enum HoNumber {
    Int(HoInt),
    Float(HoFloat),
}

impl fmt::Display for HoNumber {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            HoNumber::Int(i) => write!(formatter, "{}", i.0),
            HoNumber::Float(f) => write!(formatter, "{}", f.0)
        }
    }
}


impl HoNumber {
    pub(crate) fn float(&self) -> f64 {
        match self {
            HoNumber::Int(i) => i.float(),
            HoNumber::Float(f) => f.float(),
        }
    }
    pub(crate) fn int(&self) -> i64 {
        match self {
            HoNumber::Int(i) => i.int(),
            HoNumber::Float(f) => f.int(),
        }
    }
}

// Implement Add for Number
impl Add for HoNumber {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        match (self, other) {
            (HoNumber::Int(i), HoNumber::Int(j)) => HoNumber::Int(i + j),
            (HoNumber::Float(f), HoNumber::Float(g)) => HoNumber::Float(f + g),
            (HoNumber::Int(i), HoNumber::Float(f)) => HoNumber::Float(HoFloat(i.float()) + f),
            (HoNumber::Float(f), HoNumber::Int(i)) => HoNumber::Float(f + HoFloat(i.float())),
        }
    }
}

// Implement Sub for Number
impl Sub for HoNumber {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        match (self, other) {
            (HoNumber::Int(i), HoNumber::Int(j)) => HoNumber::Int(i - j),
            (HoNumber::Float(f), HoNumber::Float(g)) => HoNumber::Float(f - g),
            (HoNumber::Int(i), HoNumber::Float(f)) => HoNumber::Float(HoFloat(i.float()) - f),
            (HoNumber::Float(f), HoNumber::Int(i)) => HoNumber::Float(f - HoFloat(i.float())),
        }
    }
}