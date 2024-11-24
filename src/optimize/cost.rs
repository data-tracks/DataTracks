use std::cmp::Ordering;
use std::ops::{Add, Mul};

#[derive(Debug)]
pub enum  Cost {
    Numeric(usize),
    Infinite
}

impl Cost {
    pub(crate) fn new(value:usize) -> Self {
        Cost::Numeric(value)
    }
}

impl Default for Cost {
    fn default() -> Self {
        Cost::Infinite
    }
}

impl PartialEq<Self> for Cost {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Cost::Numeric(a), Cost::Numeric(b)) => a == b,
            (Cost::Infinite, Cost::Infinite) => true,
            _ => false
        }
    }
}

impl PartialOrd for Cost {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Cost::Numeric(a), Cost::Numeric(b)) => a.partial_cmp(b),
            (Cost::Infinite, Cost::Infinite) => None,
            (Cost::Infinite, Cost::Numeric(_)) => Some(Ordering::Greater),
            (Cost::Numeric(_), Cost::Infinite) => Some(Ordering::Less),
        }

    }
}

impl Add for Cost {
    type Output = Cost;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Cost::Numeric(a), Cost::Numeric(b)) => Cost::Numeric(a + b),
            (Cost::Infinite, Cost::Numeric(_)) => Cost::Infinite,
            (Cost::Numeric(_), Cost::Infinite) => Cost::Infinite,
            (Cost::Infinite, Cost::Infinite) => Cost::Infinite,
        }
    }
}

impl Mul for Cost {
    type Output = Cost;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Cost::Numeric(a), Cost::Numeric(b)) => Cost::Numeric(a * b),
            (Cost::Infinite, Cost::Numeric(_)) => Cost::Infinite,
            (Cost::Numeric(_), Cost::Infinite) => Cost::Infinite,
            (Cost::Infinite, Cost::Infinite) => Cost::Infinite,
        }
    }
}
