use std::cmp::Ordering;
use std::ops::{Add, Mul};

#[derive(Default)]
pub struct Cost {
    value: usize
}

impl Cost {
    pub(crate) fn new(value:usize) -> Self {
        Cost {value }
    }
}

impl PartialEq<Self> for Cost {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl PartialOrd for Cost {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl Add for Cost {
    type Output = Cost;

    fn add(self, rhs: Self) -> Self::Output {
        Cost::new(self.value + rhs.value)
    }
}

impl Mul for Cost {
    type Output = Cost;

    fn mul(self, rhs: Self) -> Self::Output {
        Cost::new(self.value * rhs.value)
    }
}
