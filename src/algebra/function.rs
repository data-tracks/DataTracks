use crate::algebra::function::Function::{IndexedRef, Literal, NamedRef};
use crate::algebra::Operator;
use crate::value::Value;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Function {
    Literal(LiteralOperator),
    NamedRef(NamedRefOperator),
    IndexedRef(IndexedRefOperator),
    Operation(OperationFunction),
}

impl Function {
    pub fn literal(literal: Value) -> Function {
        Literal(LiteralOperator { literal })
    }

    pub fn named_input(name: String) -> Function {
        NamedRef(NamedRefOperator { name })
    }

    pub fn indexed_input(index: u64) -> Function {
        IndexedRef(IndexedRefOperator { index })
    }
}


impl Display for Function {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Function::Literal(l) => write!(f, "{}", l.literal),
            Function::NamedRef(name) => write!(f, "${}", name),
            Function::IndexedRef(index) => write!(f, "${}", index),
            Function::Operation(op) => write!(f, "{}", op.op.dump(true)),
        }
    }
}


#[derive(Debug)]
pub struct LiteralOperator {
    pub literal: Value,
}

#[derive(Debug)]
pub struct NamedRefOperator {
    pub name: String,
}

#[derive(Debug)]
pub struct IndexedRefOperator {
    pub index: u64,
}

#[derive(Debug)]
pub struct OperationFunction {
    pub op: Operator,
    pub operands: Vec<Function>,
}