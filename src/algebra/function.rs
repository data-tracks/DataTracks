use crate::algebra::algebra::ValueHandler;
use crate::algebra::function::Operator::{IndexedInput, Input, Literal, NamedInput, Operation};
use crate::algebra::operator::Op;
use crate::algebra::BoxedValueHandler;
use crate::value::Value;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub enum Operator {
    Literal(LiteralOperator),
    NamedInput(NamedRefOperator),
    IndexedInput(IndexedRefOperator),
    Operation(OperationFunction),
    Input(InputFunction),
}


impl Operator {
    pub fn literal(literal: Value) -> Operator {
        Literal(LiteralOperator { literal })
    }

    pub fn named_input(name: String) -> Operator {
        NamedInput(NamedRefOperator { name })
    }

    pub fn indexed_input(index: usize) -> Operator {
        IndexedInput(IndexedRefOperator { index })
    }
}

pub trait Replaceable {
    fn replace<Match, Output>(&mut self, replace: fn(Match) -> Vec<Output>) -> Vec<Output>;
}

impl Replaceable for Operator {
    fn replace<Match, Output>(&mut self, replace: fn(Match) -> Vec<Output>) -> Vec<Output> {
        match self {
            Literal(l) => vec![],
            NamedInput(_) => vec![],
            IndexedInput(_) => vec![],
            Operation(o) => o.replace(replace),
            Input(_) => vec![]
        }
    }
}


pub trait Implementable<Implementation> {
    fn implement(&self) -> Result<Implementation, ()>;
}


impl Implementable<BoxedValueHandler> for Operator {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        match self {
            Literal(l) => l.implement(),
            NamedInput(n) => n.implement(),
            IndexedInput(i) => i.implement(),
            Operation(o) => o.implement(),
            Input(i) => i.implement(),
        }
    }
}


impl ValueHandler for Operator {
    fn process(&self, value: &Value) -> Value {
        match self {
            Literal(l) => l.process(value),
            NamedInput(n) => n.process(value),
            IndexedInput(i) => i.process(value),
            Input(i) => i.process(value),
            _ => panic!()
        }
    }

    fn clone(&self) -> BoxedValueHandler {
        match self {
            Literal(l) => ValueHandler::clone(l),
            NamedInput(n) => ValueHandler::clone(n),
            IndexedInput(i) => ValueHandler::clone(i),
            Input(i) => ValueHandler::clone(i),
            Operation(o) => o.implement().unwrap(),
        }
    }
}


impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal(l) => write!(f, "{}", l.literal),
            NamedInput(name) => write!(f, "${}", name.name),
            IndexedInput(index) => write!(f, "${}", index.index),
            Operation(op) => write!(f, "{}", op.op.dump(true)),
            Input(_) => write!(f, "!"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InputFunction {
    index: usize,
    all: bool,
}

impl InputFunction {
    pub fn new(index: usize) -> Self {
        InputFunction {
            index,
            all: false,
        }
    }

    pub fn all() -> Self {
        InputFunction {
            index: 0,
            all: true,
        }
    }
}

impl ValueHandler for InputFunction {
    fn process(&self, value: &Value) -> Value {
        if self.all {
            return value.clone();
        }
        match value {
            Value::Array(a) => {
                a.0.get(self.index).unwrap_or(&Value::null()).clone()
            }
            Value::Dict(d) => {
                d.0.get(&format!("${}", self.index)).unwrap_or(&Value::null()).clone()
            }
            Value::Null => Value::null(),
            _ => panic!("Could not process {}", value)
        }
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(InputFunction { index: self.index, all: self.all })
    }
}

impl Implementable<BoxedValueHandler> for InputFunction {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(ValueHandler::clone(self))
    }
}


#[derive(Debug, Clone)]
pub struct LiteralOperator {
    pub literal: Value,
}

impl LiteralOperator {
    pub fn new(literal: Value) -> LiteralOperator {
        LiteralOperator { literal }
    }
}


impl ValueHandler for LiteralOperator {
    fn process(&self, _value: &Value) -> Value {
        self.literal.clone()
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(LiteralOperator { literal: self.literal.clone() })
    }
}

impl Implementable<BoxedValueHandler> for LiteralOperator {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(ValueHandler::clone(self))
    }
}

#[derive(Debug, Clone)]
pub struct NamedRefOperator {
    pub name: String,
}

impl NamedRefOperator {
    pub fn new(name: String) -> NamedRefOperator {
        NamedRefOperator { name }
    }
}

impl ValueHandler for NamedRefOperator {
    fn process(&self, value: &Value) -> Value {
        match value {
            Value::Dict(d) => d.0.get(&self.name).unwrap_or(&Value::null()).clone(),
            Value::Null => Value::null(),
            v => panic!("Could not process {}", v)
        }
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(NamedRefOperator { name: self.name.clone() })
    }
}

impl Implementable<BoxedValueHandler> for NamedRefOperator {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(ValueHandler::clone(self))
    }
}


#[derive(Debug, Clone)]
pub struct IndexedRefOperator {
    pub index: usize,
}

impl ValueHandler for IndexedRefOperator {
    fn process(&self, value: &Value) -> Value {
        match value {
            Value::Array(a) => a.0.get(self.index).cloned().unwrap(),
            Value::Null => Value::null(),
            _ => panic!()
        }
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(IndexedRefOperator { index: self.index })
    }
}

impl Implementable<BoxedValueHandler> for IndexedRefOperator {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(ValueHandler::clone(self))
    }
}


#[derive(Debug, Clone)]
pub struct OperationFunction {
    pub op: Op,
    pub operands: Vec<Operator>,
}


impl OperationFunction {
    pub fn new(op: Op, operands: Vec<Operator>) -> OperationFunction {
        OperationFunction { op, operands }
    }
}

impl Replaceable for OperationFunction {
    fn replace<Match, Output>(&mut self, replace: fn(Match) -> Vec<Output>) -> Vec<Output> {
        match &self.op {
            Op::Agg(_) => replace(self),
            Op::Tuple(_) => self.operands.iter().map(|mut o| o.replace(replace)).collect(),
        }
    }
}


impl Implementable<BoxedValueHandler> for OperationFunction {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(self.op.implement(self.operands.clone()))
    }
}
