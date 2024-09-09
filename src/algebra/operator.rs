use crate::algebra::Operator::{Divide, Minus, Multiplication, Plus};
use crate::value::Value;
use std::str::FromStr;

#[derive(Debug)]
pub enum Operator {
    Plus(PlusOperator),
    Minus(MinusOperator),
    Multiplication(MultiplicationOperator),
    Divide(DivideOperator),

}

impl Operator {
    pub fn implement(&self, operators: Vec<fn(Value) -> Value>) -> fn(Value) -> Value {
        match self {
            Plus(_) => {
                |v| {
                    operators.iter().fold(Value::int(0), |a, b| {
                        &a + &b(v)
                    })
                }
            }
            Minus(_) => {
                |v| {
                    operators.iter().fold(Value::int(0), |a, b| {
                        &a - &b(v)
                    })
                }
            }
            Multiplication(_) => {
                |v| {
                    operators.iter().fold(Value::int(0), |a, b| {
                        &a * &b(v)
                    })
                }
            }
            Divide(_) => {
                |v| {
                    operators.iter().fold(Value::int(0), |a, b| {
                        &a / &b(v)
                    })
                }
            }
        }
    }

    pub fn dump(&self, as_call: bool) -> String {
        match self {
            Plus(_) => {
                if as_call {
                    String::from("ADD")
                } else {
                    String::from("+")
                }
            }
            Minus(_) => {
                if as_call {
                    String::from("MINUS")
                } else {
                    String::from("-")
                }
            }
            Multiplication(_) => {
                if as_call {
                    String::from("MULTIPLICATION")
                } else {
                    String::from("*")
                }
            }
            Divide(_) => {
                if as_call {
                    String::from("DIVIDE")
                } else {
                    String::from("/")
                }
            }
            _ => panic!("Unexpected case!"),
        }
    }
}

impl FromStr for Operator {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut trimmed = s.to_lowercase();
        if s.ends_with("(") {
            trimmed.pop();
        }
        match trimmed.as_str() {
            "+" | "add" | "plus" => Ok(Plus(PlusOperator)),
            "-" | "minus" => Ok(Minus(MinusOperator)),
            "*" | "multiply" => Ok(Multiplication(MultiplicationOperator)),
            "/" | "divide" => Ok(Divide(DivideOperator)),
            _ => Err(())
        }
    }
}


impl Operator {
    pub fn plus() -> Operator {
        Plus(PlusOperator)
    }
    pub fn minus() -> Operator {
        Minus(MinusOperator)
    }
    pub fn multiplication() -> Operator {
        Multiplication(MultiplicationOperator)
    }
    pub fn divide() -> Operator {
        Divide(DivideOperator)
    }
}


#[derive(Debug)]
pub struct PlusOperator;

#[derive(Debug)]
pub struct MinusOperator;
#[derive(Debug)]
pub struct MultiplicationOperator;

#[derive(Debug)]
pub struct DivideOperator;

