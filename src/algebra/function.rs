use crate::algebra::algebra::ValueHandler;
use crate::algebra::function::Function::{IndexedInput, Literal, NamedInput, Operation};
use crate::algebra::Function::Input;
use crate::algebra::Operator;
use crate::value::Value;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub enum Function {
    Literal(LiteralOperator),
    NamedInput(NamedRefOperator),
    IndexedInput(IndexedRefOperator),
    Operation(OperationFunction),
    Input(InputFunction)
}

impl Function {
    pub fn literal(literal: Value) -> Function {
        Literal(LiteralOperator { literal })
    }

    pub fn named_input(name: String) -> Function {
        NamedInput(NamedRefOperator { name })
    }

    pub fn indexed_input(index: usize) -> Function {
        IndexedInput(IndexedRefOperator { index })
    }
}


impl ValueHandler for Function {
    fn process(&self, value: Value) -> Value {
        match self {
            Literal(l) => l.process(value),
            NamedInput(n) => n.process(value),
            IndexedInput(i) => i.process(value),
            Operation(o) => o.process(value),
            Input(i) => i.process(value),
        }
    }

    fn clone(&self) -> Box<dyn ValueHandler + Send + 'static> {
        match self {
            Literal(l) => ValueHandler::clone(l),
            NamedInput(n) => ValueHandler::clone(n),
            IndexedInput(i) => ValueHandler::clone(i),
            Operation(o) => ValueHandler::clone(o),
            Input(i) => ValueHandler::clone(i),
        }
    }
}


impl Display for Function {
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
    fn process(&self, value: Value) -> Value {
        if self.all {
            return value
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

    fn clone(&self) -> Box<dyn ValueHandler + Send + 'static> {
        Box::new(InputFunction::new(self.index))
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

impl NamedRefOperator {
    pub fn new(name: String) -> NamedRefOperator {
        NamedRefOperator { name }
    }
}

impl ValueHandler for NamedRefOperator {
    fn process(&self, value: Value) -> Value {
        match value {
            Value::Dict(d) => d.0.get(&self.name).unwrap_or(&Value::null()).clone(),
            Value::Null => Value::null(),
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
            Value::Null => Value::null(),
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

impl OperationFunction {
    pub fn new(op: Operator, operands: Vec<Function>) -> OperationFunction {
        OperationFunction { op, operands }
    }
}

impl ValueHandler for OperationFunction {
    fn process(&self, value: Value) -> Value {
        self.op.implement(self.operands.iter().map(|v| v.process(value.clone())).collect())
    }

    fn clone(&self) -> Box<dyn ValueHandler + Send + 'static> {
        Box::new(OperationFunction { op: self.op.clone(), operands: self.operands.iter().map(Clone::clone).collect() })
    }
}
