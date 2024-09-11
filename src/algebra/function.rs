use crate::algebra::algebra::ValueHandler;
use crate::algebra::function::Function::{IndexedRef, Literal, NamedRef, Operation};
use crate::algebra::Operator;
use crate::value::Value;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
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

    pub fn indexed_input(index: usize) -> Function {
        IndexedRef(IndexedRefOperator { index })
    }
}


impl ValueHandler for Function {
    fn process(&self, value: Value) -> Value {
        match self {
            Literal(l) => l.process(value),
            NamedRef(n) => n.process(value),
            IndexedRef(i) => i.process(value),
            Operation(o) => o.process(value),
        }
    }

    fn clone(&self) -> Box<dyn ValueHandler + Send + 'static> {
        match self {
            Literal(l) => ValueHandler::clone(l),
            NamedRef(n) => ValueHandler::clone(n),
            IndexedRef(i) => ValueHandler::clone(i),
            Operation(o) => ValueHandler::clone(o)
        }
    }
}


impl Display for Function {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal(l) => write!(f, "{}", l.literal),
            NamedRef(name) => write!(f, "${}", name.name),
            IndexedRef(index) => write!(f, "${}", index.index),
            Operation(op) => write!(f, "{}", op.op.dump(true)),
        }
    }
}


#[derive(Debug, Clone)]
pub struct LiteralOperator {
    pub literal: Value,
}


impl ValueHandler for LiteralOperator {
    fn process(&self, _value: Value) -> Value {
        self.literal.clone()
    }

    fn clone(&self) -> Box<dyn ValueHandler + Send + 'static> {
        Box::new(LiteralOperator { literal: self.literal.clone() })
    }
}

#[derive(Debug, Clone)]
pub struct NamedRefOperator {
    pub name: String,
}

impl ValueHandler for NamedRefOperator {
    fn process(&self, value: Value) -> Value {
        match value {
            Value::Dict(d) => d.0.get(&self.name).unwrap().clone(),
            Value::Null(_) => Value::null(),
            _ => panic!()
        }
    }

    fn clone(&self) -> Box<dyn ValueHandler + Send + 'static> {
        Box::new(NamedRefOperator { name: self.name.clone() })
    }
}


#[derive(Debug, Clone)]
pub struct IndexedRefOperator {
    pub index: usize,
}

impl ValueHandler for IndexedRefOperator {
    fn process(&self, value: Value) -> Value {
        match value {
            Value::Array(a) => a.0.get(self.index).cloned().unwrap(),
            Value::Null(_) => Value::null(),
            _ => panic!()
        }
    }

    fn clone(&self) -> Box<dyn ValueHandler + Send + 'static> {
        Box::new(IndexedRefOperator { index: self.index })
    }
}


#[derive(Debug, Clone)]
pub struct OperationFunction {
    pub op: Operator,
    pub operands: Vec<Function>,
}

impl ValueHandler for OperationFunction {
    fn process(&self, value: Value) -> Value {
        self.op.implement(self.operands.iter().map(|v| v.process(value.clone())).collect())
    }

    fn clone(&self) -> Box<dyn ValueHandler + Send + 'static> {
        Box::new(OperationFunction { op: self.op.clone(), operands: self.operands.iter().map(|o| Clone::clone(o)).collect() })
    }
}
