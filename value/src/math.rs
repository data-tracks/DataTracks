use std::ops::{Add, AddAssign, Div, Mul, Sub};
use tracing::error;
use crate::Value;

impl Add for &Value {
    type Output = Value;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            // Case where both are integers
            (Value::Int(a), Value::Int(b)) => Value::Int(*a + *b),

            // Mixing Integer and Float, ensure the result is a Float
            (Value::Int(a), Value::Float(b)) | (Value::Float(b), Value::Int(a)) => {
                Value::Float(*a + *b)
            }
            (Value::Float(a), Value::Float(b)) => Value::Float(*a + *b),
            // text
            (Value::Text(a), b) => {
                let b = b.as_text().unwrap();
                Value::text(&format!("{}{}", a.0, b.0))
            }
            // time
            (Value::Time(a), b) => {
                let ms = a.ms + b.as_time().unwrap().ms;
                let ns = a.ns + b.as_time().unwrap().ns;
                Value::time(ms, ns)
            }
            (Value::Date(a), b) => Value::date(a.days + b.as_date().unwrap().days),
            // array
            (Value::Array(a), b) => {
                let mut a = a.clone();
                a.values.push(b.clone());
                Value::Array(a)
            }

            // Panic on unsupported types
            (lhs, rhs) => panic!("Cannot add {lhs:?} with {rhs:?}."),
        }
    }
}

impl Sub for &Value {
    type Output = Value;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Int(a), Value::Int(b)) => Value::int(a.0 - b.0),
            (Value::Int(_), Value::Float(b)) => {
                let right = Value::float_parts(-b.number, b.shift);
                right.add(self)
            }
            (Value::Float(_), Value::Int(b)) => Value::int(-b.0).add(self),
            (lhs, rhs) => panic!("Cannot subtract {:?} from {:?}.", lhs, rhs),
        }
    }
}

impl Mul for &Value {
    type Output = Value;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Int(a), Value::Int(b)) => Value::int(a.0 * b.0),
            (Value::Int(a), Value::Float(b)) | (Value::Float(b), Value::Int(a)) => {
                Value::float_parts(a.0 * b.number, b.shift)
            }
            (Value::Float(a), Value::Float(b)) => {
                let max = a.shift.max(b.shift);
                let shift_diff = a.shift.abs_diff(b.shift) as i64;
                Value::float_parts(a.number * b.number * (10 ^ shift_diff), max)
            }
            (Value::Text(text), Value::Int(b)) => Value::text(&text.0.repeat(b.0 as usize)),
            (lhs, rhs) => panic!("Cannot multiply {:?} with {:?}.", lhs, rhs),
        }
    }
}

impl Div for &Value {
    type Output = Value;

    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Int(a), Value::Int(b)) => Value::float(a.0 as f64 / b.0 as f64),
            (Value::Int(a), Value::Float(b)) => Value::float(a.0 as f64 / b.as_f64()),
            (Value::Float(a), Value::Int(b)) => Value::float(a.as_f64() / b.0 as f64),
            (Value::Float(a), Value::Float(b)) => Value::float(a.as_f64() / b.as_f64()),
            _ => panic!("Cannot divide {:?} with {:?}.", self, rhs),
        }
    }
}

impl AddAssign for Value {
    fn add_assign(&mut self, rhs: Self) {
        match self {
            Value::Int(i) => {
                i.0 += rhs.as_int().unwrap().0;
            }
            Value::Float(f) => {
                let rhs = rhs.as_float().unwrap();
                let diff = f.shift.abs_diff(rhs.shift);
                match (f, rhs) {
                    (l, r) if l.shift > r.shift => {
                        l.number += r.number * (10 ^ diff) as i64;
                    }
                    (l, r) if l.shift < r.shift => {
                        l.number = l.number * (10 ^ diff) as i64 + r.number;
                        l.shift = r.shift;
                    }
                    (l, r) => {
                        l.number += r.number;
                    }
                }
            }
            Value::Bool(b) => b.0 = b.0 && rhs.as_bool().unwrap().0,
            Value::Text(t) => t.0 += &rhs.as_text().unwrap().0,
            Value::Array(a) => a.values.push(rhs),
            Value::Dict(d) => d.append(&mut rhs.as_dict().unwrap()),
            Value::Null => {}
            Value::Time(t) => {
                let time = rhs.as_time().unwrap();
                t.ms += time.ms;
                t.ns += time.ns;
            }
            Value::Date(d) => {
                d.days += rhs.as_date().unwrap().days;
            }
            Value::Node(_) => {
                error!("Cannot add Node");
            }
            Value::Edge(_) => {
                error!("Cannot add Edge");
            }
        }
    }
}