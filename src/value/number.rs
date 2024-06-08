use std::ops::{Add, Sub};

pub trait Number: Add<Self, Output=Self> + Sub<Self, Output=Self> + Sized {
    fn float(&self) -> f64;
    fn int(&self) -> i64;
}

