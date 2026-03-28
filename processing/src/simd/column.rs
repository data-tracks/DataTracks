use value::{Float, Int, Text, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum Column {
    Text(Vec<Text>),
    Int(Vec<Int>),
    Float(Vec<Float>),
}

impl From<(Value, usize)> for Column {
    fn from(value: (Value, usize)) -> Self {
        let (value, size) = value;
        match value {
            Value::Int(num) => {
                Column::Int(vec![num; size])
            }
            Value::Float(num) => {
                Column::Float(vec![num; size])
            }
            Value::Text(text) => {
                Column::Text(vec![text; size])
            }
            _ => unimplemented!(),
        }
    }
}
