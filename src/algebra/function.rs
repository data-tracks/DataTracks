use crate::algebra::algebra::BoxedValueLoader;
use crate::algebra::operator::{AggOp, InputOp, LiteralOp, NameOp, Op};
use crate::algebra::BoxedValueHandler;
use crate::algebra::Op::Tuple;
use crate::algebra::TupleOp::{Combine, Input, Literal, Name};
use crate::value::Value;

pub trait Replaceable {
    fn replace(&mut self, replace: fn(&mut Operator) -> Vec<(AggOp, Vec<Operator>)>) -> Vec<(AggOp, Vec<Operator>)>;
}

pub trait Implementable<Implementation> {
    fn implement(&self) -> Result<Implementation, ()>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct Operator {
    pub op: Op,
    pub operands: Vec<Operator>,
}


impl Operator {
    pub fn new(op: Op, operands: Vec<Operator>) -> Operator {
        Operator { op, operands }
    }

    pub fn name(name: &str) -> Operator {
        Operator { op: Tuple(Name(NameOp::new(name.to_string()))), operands: vec![] }
    }

    // $0.1 -> 1
    pub fn index(index: usize) -> Self {
        Operator { op: Op::index(index), operands: vec![] }
    }
    // $0 -> 0
    pub fn input() -> Self {
        Operator { op: Tuple(Input(InputOp {})), operands: vec![] }
    }

    pub fn literal(literal: Value) -> Self {
        Operator { op: Tuple(Literal(LiteralOp { literal })), operands: vec![] }
    }

    pub(crate) fn combine(values: Vec<Operator>) -> Self {
        Operator { op: Tuple(Combine), operands: values }
    }
}

impl Replaceable for Operator {
    fn replace(&mut self, replace: fn(&mut Operator) -> Vec<(AggOp, Vec<Operator>)>) -> Vec<(AggOp, Vec<Operator>)> {
        match &self.op {
            Op::Agg(_) => replace(self),
            Tuple(_) => self.operands.iter_mut().flat_map(|o| o.replace(replace)).collect()
        }
    }
}


impl Implementable<BoxedValueHandler> for Operator {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        match &self.op {
            Op::Agg(_) => Err(()),
            Tuple(t) => Ok(t.implement(self.operands.clone()))
        }
    }
}

impl Implementable<BoxedValueLoader> for Operator {
    fn implement(&self) -> Result<BoxedValueLoader, ()> {
        match &self.op {
            Op::Agg(a) => a.implement(),
            Tuple(_) => Err(())
        }
    }
}
