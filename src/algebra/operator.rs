use crate::algebra::Operator::{Divide, Minus, Multiplication, Plus};

#[derive(Debug)]
pub enum Operator {
    Plus(PlusOperator),
    Minus(MinusOperator),
    Multiplication(MultiplicationOperator),
    Divide(DivideOperator),
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