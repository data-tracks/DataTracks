use crate::value::{HoFloat, HoInt};
use crate::value::string::HoString;

pub enum ValType{
    Integer,
    Float,
    String
}

pub enum Value{
    Int(HoInt),
    Float(HoFloat),
    String(HoString)
}


pub trait Valuable {
    fn type_(&self) -> ValType;
}