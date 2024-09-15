use crate::algebra::Operator::{Divide, Equal, Minus, Multiplication, Not, Plus};
use crate::value::Value;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum Operator {
    Plus,
    Minus,
    Multiplication,
    Divide,
    Combine,
    Not,
    Equal,
    And
}

impl Operator {
    pub fn implement(&self, operators: Vec<Value>) -> Value {
        match self {
            Plus => {
                operators.iter().fold(Value::int(0), |a, b| {
                    &a + b
                })
            }
            Minus => {
                operators.iter().fold(Value::int(0), |a, b| {
                    &a - &b
                })
            }
            Multiplication => {
                operators.iter().fold(Value::int(0), |a, b| {
                    &a * &b
                })
            }
            Divide => {
                operators.iter().fold(Value::int(0), |a, b| {
                    &a / &b
                })
            }
            Equal => {
                operators.into_iter().fold(Value::bool(true), |a, b| {
                    (&a == &b).into()
                })
            }
            Operator::Combine => {
                Value::array(operators)
            }
            Not => {
                operators.into_iter().fold(Value::bool(false), |a, b| {
                    (!(&a == &b)).into()
                })
            }
            Operator::And => {
                operators.into_iter().fold(Value::bool(true), |a, b| {
                    (a.as_bool().unwrap().0 && b.as_bool().unwrap().0).into()
                })
            }
        }
    }
    pub fn dump(&self, as_call: bool) -> String {
        match self {
            Plus => {
                if as_call {
                    String::from("ADD")
                } else {
                    String::from("+")
                }
            }
            Minus => {
                if as_call {
                    String::from("MINUS")
                } else {
                    String::from("-")
                }
            }
            Multiplication => {
                if as_call {
                    String::from("MULTIPLICATION")
                } else {
                    String::from("*")
                }
            }
            Divide => {
                if as_call {
                    String::from("DIVIDE")
                } else {
                    String::from("/")
                }
            }
            Operator::Combine => {
                String::from("")
            }
            Not => {
                String::from("NOT")
            }
            Equal => {
                if as_call {
                    String::from("EQ")
                } else {
                    String::from("=")
                }
            }
            Operator::And => {
                String::from("AND")
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
            "+" | "add" | "plus" => Ok(Plus),
            "-" | "minus" => Ok(Minus),
            "*" | "multiply" => Ok(Multiplication),
            "/" | "divide" => Ok(Divide),
            _ => Err(())
        }
    }
}


impl Operator {
    pub fn plus() -> Operator {
        Plus
    }
    pub fn minus() -> Operator {
        Minus
    }
    pub fn multiply() -> Operator {
        Multiplication
    }
    pub fn divide() -> Operator {
        Divide
    }

    pub fn equal() -> Operator {
        Equal
    }

    pub fn not() -> Operator {
        Not
    }
}



