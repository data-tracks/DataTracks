use std::fmt::Display;
use std::fmt::Formatter;

use crate::value::{HoFloat, HoInt};
use crate::value::string::HoString;

pub enum ValType{
    Integer,
    Float,
    String
}

pub enum Value {
    Int(HoInt),
    Float(HoFloat),
    String(Box<HoString>),
}


impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(v) => v.fmt(f),
            Value::Float(v) => v.fmt(f),
            Value::String(v) => v.fmt(f),
        }
    }
}

pub trait Valuable {
    fn type_(&self) -> ValType;
}