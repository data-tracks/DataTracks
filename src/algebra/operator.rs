use std::fmt::Display;
use std::str::FromStr;
use crate::algebra::Operator::{Divide, Minus, Multiplication, Plus};

#[derive(Debug)]
pub enum Operator {
    Plus(PlusOperator),
    Minus(MinusOperator),
    Multiplication(MultiplicationOperator),
    Divide(DivideOperator),
}

impl Operator {
    pub fn dump(&self, as_call: bool ) -> &str {
        if as_call {
            return match self {
                Plus(_) => "ADD",
                Minus(_) => "MINUS",
                Multiplication(_) => "MULTIPLICATION",
                Divide(_) => "DIVIDE"
            }
        };
        match self {
            Plus(_) => "+",
            Minus(_) => "-",
            Multiplication(_) => "*",
            Divide(_) => "/"
        }
    }
}

impl FromStr for Operator{
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut trimmed = s.to_lowercase();
        if s.ends_with("(")  {
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