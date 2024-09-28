use crate::algebra::algebra::ValueHandler;
use crate::algebra::function::Implementable;
use crate::algebra::operator::Function::Tuple;
use crate::algebra::operator::Op::{Divide, Equal, Minus, Multiplication, Not, Plus};
use crate::algebra::{BoxedValueHandler, Operator};
use crate::value::Value;
use crate::value::Value::{Array, Bool, Dict, Float, Int, Null, Text};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum Op {
    Plus,
    Minus,
    Multiplication,
    Divide,
    Combine,
    Not,
    Equal,
    And,
    Or,
}


impl Op {
    pub fn implement(&self, operators: Vec<Operator>) -> Function {
        let operands = operators.into_iter().map(|o| o.implement()).collect();
        match self {
            Plus => {
                Tuple(
                    TupleFunction::new(|value| {
                        value.iter().fold(Value::int(0), |a, b| {
                            &a + b
                        })
                    }, operands)
                )
            }
            Minus => {
                Tuple(
                    TupleFunction::new(move |value| {
                        let a = value.get(0).unwrap();
                        let b = value.get(1).unwrap();
                        a - b
                    }, operands)
                )
            }
            Multiplication => {
                Tuple(
                    TupleFunction::new(move |value| {
                        value.iter().fold(Value::int(1), |a, b| {
                            &a * b
                        })
                    }, operands)
                )
            }
            Divide => {
                Tuple(
                    TupleFunction::new(move |value| {
                        let a = value.get(0).unwrap();
                        let b = value.get(1).unwrap();
                        a / b
                    }, operands)
                )
            }
            Equal => {
                Tuple(
                    TupleFunction::new(move |value| {
                        let a = value.get(0).unwrap();
                        let b = value.get(1).unwrap();
                        (a.clone() == b.clone()).into()
                    }, operands)
                )
            }
            Op::Combine => {
                Tuple(TupleFunction::new(move |value| {
                    Value::array(value.iter().map(|v| (*v).clone()).collect())
                }, operands))
            }
            Not => {
                Tuple(TupleFunction::new(move |vec| {
                    let value = Value::bool(vec.get(0).unwrap().as_bool().unwrap().0);
                    match vec.get(0).unwrap() {
                        Int(_) => Int(value.as_int().unwrap()),
                        Float(_) => Float(value.as_float().unwrap()),
                        Bool(_) => Bool(value.as_bool().unwrap()),
                        Text(_) => Text(value.as_text().unwrap()),
                        Array(_) => Array(value.as_array().unwrap()),
                        Dict(_) => Dict(value.as_dict().unwrap()),
                        Null => Value::null()
                    }
                }, operands))
            }
            Op::And => {
                Tuple(TupleFunction::new(move |value| {
                    value.iter().fold(Value::bool(true), |a, b| {
                        (a.as_bool().unwrap().0 && b.as_bool().unwrap().0).into()
                    })
                }, operands))
            }
            Op::Or => {
                Tuple(TupleFunction::new(move |value| {
                    value.iter().fold(Value::bool(true), |a, b| {
                        (a.as_bool().unwrap().0 || b.as_bool().unwrap().0).into()
                    })
                }, operands))
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
            Op::Combine => {
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
            Op::And => {
                String::from("AND")
            }
            Op::Or => {
                String::from("OR")
            }
        }
    }
}

pub enum Function {
    Tuple(TupleFunction)
}

impl Implementable for Function {
    type Result = BoxedValueHandler;

    fn implement(&self) -> Self::Result {
        match self {
            Tuple(t) => t.clone(),
        }
    }
}


pub struct TupleFunction {
    func: fn(&Vec<Value>) -> Value,
    children: Vec<BoxedValueHandler>,
}


impl TupleFunction {
    pub fn new(func: fn(&Vec<Value>) -> Value, children: Vec<BoxedValueHandler>) -> Self {
        TupleFunction { func, children }
    }
}

impl ValueHandler for TupleFunction {
    fn process(&self, value: &Value) -> Value {
        let children = self.children.iter().map(|c| c.process(value)).collect();
        (self.func)(&children)
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(TupleFunction::new(self.func, self.children.iter().map(|c| (*c).clone()).collect()))
    }
}


impl FromStr for Op {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut trimmed = s.to_lowercase();
        if s.ends_with('(') {
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


impl Op {
    pub fn plus() -> Op {
        Plus
    }
    pub fn minus() -> Op {
        Minus
    }
    pub fn multiply() -> Op {
        Multiplication
    }
    pub fn divide() -> Op {
        Divide
    }

    pub fn equal() -> Op {
        Equal
    }

    pub fn not() -> Op {
        Not
    }
}




