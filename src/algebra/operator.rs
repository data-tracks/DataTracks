use crate::algebra::Operator::{Divide, Minus, Multiplication, Plus};
use crate::value::Value;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum Operator {
    Plus(PlusOperator),
    Minus(MinusOperator),
    Multiplication(MultiplicationOperator),
    Divide(DivideOperator),

}

impl Operator {
    pub fn implement(&self, operators: Vec<Value>) -> Value {
        match self {
            Plus(_) => {
                operators.iter().fold(Value::int(0), |a, b| {
                    &a + b
                })
            }
            Minus(_) => {
                operators.iter().fold(Value::int(0), |a, b| {
                    &a - &b
                })
            }
            Multiplication(_) => {
                operators.iter().fold(Value::int(0), |a, b| {
                    &a * &b
                })
            }
            Divide(_) => {
                operators.iter().fold(Value::int(0), |a, b| {
                    &a / &b
                })
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


#[derive(Debug, Clone)]
pub struct PlusOperator;

#[derive(Debug, Clone)]
pub struct MinusOperator;
#[derive(Debug, Clone)]
pub struct MultiplicationOperator;

#[derive(Debug, Clone)]
pub struct DivideOperator;

