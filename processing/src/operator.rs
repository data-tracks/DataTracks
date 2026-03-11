use crate::expression::Expression;
use crate::operator::Operator::{B, S};
use value::Value;

#[derive(Clone)]
pub enum Operator {
    S(Single),
    B(Binary),
}

impl Operator {
    pub(crate) fn eval(&self, expressions: Vec<Expression>) -> Box<dyn Fn(&Value) -> Value> {
        match self {
            S(s) => s.eval(expressions),
            B(b) => b.eval(expressions),
        }
    }
}

impl Operator {
    pub fn single(s: Single) -> Self {
        S(s)
    }

    pub fn binary(binary: Binary) -> Self {
        B(binary)
    }
}

#[derive(Clone)]
pub enum Binary {
    Add,
    Minus,
}

impl Binary {
    pub(crate) fn eval(&self, expressions: Vec<Expression>) -> Box<dyn Fn(&Value) -> Value> {
        let val1 = expressions[0].eval(expressions.clone());
        let val2 = expressions[1].eval(expressions.clone());

        match self {
            Binary::Add => Box::new(move |v| &val1(v) + &val2(v)),
            Binary::Minus => Box::new(move |v| &val1(v) - &val2(v)),
        }
    }
}

#[derive(Clone)]
pub enum Single {
    Length,
}

impl Single {
    pub(crate) fn eval(&self, expressions: Vec<Expression>) -> Box<dyn Fn(&Value) -> Value> {
        todo!()
    }
}
