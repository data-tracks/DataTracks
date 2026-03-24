use value::{Float, Int, Text};

#[derive(Clone, Debug, PartialEq)]
pub enum Column {
    Text(Vec<Text>),
    Int(Vec<Int>),
    Float(Vec<Float>),
}
