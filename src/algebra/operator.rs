use crate::algebra::aggregate::CountOperator;
use crate::algebra::algebra::{BoxedValueLoader, ValueHandler};
use crate::algebra::function::Implementable;
use crate::algebra::operator::AggOp::Count;
use crate::algebra::operator::TupleOp::{Divide, Equal, Minus, Multiplication, Not, Plus};
use crate::algebra::Op::Tuple;
use crate::algebra::TupleOp::{And, Combine, Or};
use crate::algebra::{BoxedValueHandler, Operator};
use crate::value::Value;
use crate::value::Value::{Array, Bool, Dict, Float, Int, Null, Text};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum Op {
    Agg(AggOp),
    Tuple(TupleOp),
}


impl Op {
    pub(crate) fn dump(&self, as_call: bool) -> String {
        match self {
            Op::Agg(a) => a.dump(as_call),
            Tuple(t) => t.dump(as_call)
        }
    }

    pub(crate) fn implement(&self, operators: Vec<Operator>) -> BoxedValueHandler {
        match self {
            Op::Agg(_) => panic!("Aggregations should have been replaced!"),
            Tuple(t) => t.implement(operators)
        }
    }
}

#[derive(Debug, Clone)]
pub enum TupleOp {
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


impl TupleOp {
    pub fn implement(&self, operators: Vec<Operator>) -> BoxedValueHandler {
        let operands = operators.into_iter().map(|o| o.implement().unwrap()).collect();
        match self {
            Plus => {
                Box::new(
                    TupleFunction::new(|value| {
                        value.iter().fold(Value::int(0), |a, b| {
                            &a + b
                        })
                    }, operands)
                )
            }

            Minus => {
                Box::new(
                    TupleFunction::new(move |value| {
                        let a = value.get(0).unwrap();
                        let b = value.get(1).unwrap();
                        a - b
                    }, operands)
                )
            }
            Multiplication => {
                Box::new(
                    TupleFunction::new(move |value| {
                        value.iter().fold(Value::int(1), |a, b| {
                            &a * b
                        })
                    }, operands)
                )
            }
            Divide => {
                Box::new(
                    TupleFunction::new(move |value| {
                        let a = value.get(0).unwrap();
                        let b = value.get(1).unwrap();
                        a / b
                    }, operands)
                )
            }
            Equal => {
                Box::new(
                    TupleFunction::new(move |value| {
                        let a = value.get(0).unwrap();
                        let b = value.get(1).unwrap();
                        (a.clone() == b.clone()).into()
                    }, operands)
                )
            }
            Combine => {
                Box::new(
                    TupleFunction::new(move |value| {
                        Value::array(value.iter().map(|v| (*v).clone()).collect())
                    }, operands)
                )
            }
            Not => {
                Box::new(
                    TupleFunction::new(move |vec| {
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
                    }, operands)
                )
            }
            And => {
                Box::new(
                    TupleFunction::new(move |value| {
                        value.iter().fold(Value::bool(true), |a, b| {
                            (a.as_bool().unwrap().0 && b.as_bool().unwrap().0).into()
                        })
                    }, operands)
                )
            }
            Or => {
                Box::new(
                    TupleFunction::new(move |value| {
                        value.iter().fold(Value::bool(true), |a, b| {
                            (a.as_bool().unwrap().0 || b.as_bool().unwrap().0).into()
                        })
                    }, operands)
                )
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
            TupleOp::Combine => {
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
            And => {
                String::from("AND")
            }
            Or => {
                String::from("OR")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum AggOp {
    Count
}

impl AggOp {
    pub(crate) fn dump(&self, _as_call: bool) -> String {
        match self {
            Count => "COUNT".to_string(),
        }
    }
}

impl Implementable<BoxedValueLoader> for AggOp {
    fn implement(&self) -> Result<BoxedValueLoader, ()> {
        match self {
            Count => Ok(Box::new(CountOperator::new()))
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
            "+" | "add" | "plus" => Ok(Tuple(Plus)),
            "-" | "minus" => Ok(Tuple(Minus)),
            "*" | "multiply" => Ok(Tuple(Multiplication)),
            "/" | "divide" => Ok(Tuple(Divide)),
            _ => Err(())
        }
    }
}

impl Op {
    pub fn plus() -> Op {
        Tuple(Plus)
    }
    pub fn minus() -> Op {
        Tuple(Minus)
    }
    pub fn multiply() -> Op {
        Tuple(Multiplication)
    }
    pub fn divide() -> Op {
        Tuple(Divide)
    }

    pub fn equal() -> Op {
        Tuple(Equal)
    }

    pub fn not() -> Op {
        Tuple(Not)
    }

    pub fn and() -> Op {
        Tuple(And)
    }
    pub fn or() -> Op {
        Tuple(Or)
    }

    pub(crate) fn combine() -> Op {
        Tuple(Combine)
    }
}




