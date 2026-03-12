use sqlparser::ast::BinaryOperator;
use crate::program::VM;
use value::Value;
use crate::expression::Expression;
use crate::language::Sql;

pub enum Step {
    Next, // IP + 1
    Stay // We stay
}

#[derive(Clone)]
pub enum Operator {
    Add,
    Minus,
    Multiply,
    Gt,
    Index,
    Explode
}

impl Operator {
    pub(crate) fn sql(&self, expressions: Vec<Expression>) -> String {
        match self {
            Operator::Add => format!("{} + {}", expressions[0].sql(), expressions[1].sql()),
            Operator::Minus => format!("{} - {}", expressions[0].sql(), expressions[1].sql()),
            Operator::Multiply => format!("{} * {}", expressions[0].sql(), expressions[1].sql()),
            Operator::Gt => format!("{} > {}", expressions[0].sql(), expressions[1].sql()),
            Operator::Index => format!("{}[{}]", expressions[0].sql(), expressions[1].sql()),
            Operator::Explode => format!("explode({})", expressions[0].sql()),
        }
    }
}


fn op_index(vm: &mut VM, _: usize) {
    let index = vm.stack.pop().expect("Stack underflow").as_int().unwrap().0 as usize;
    let array = vm.stack.pop().expect("Stack underflow");
    if let Value::Array(a) = array {
        vm.stack.push(a.values[index].clone());
    }else if let Value::Text(t) = array{
        vm.stack.push(Value::text(&t.0[index..index + 1]))
    }
}

#[derive(Clone)]
pub enum Binary {
    Add,
    Sub,
    Index,
    Multiply,
}

impl Binary {
    pub(crate) fn sql(&self, s0: Expression, s1: Expression) -> String {
        let s0 = s0.sql();
        let s1 = s1.sql();

        match self {
            Binary::Add => {
                format!("{}+{}", s0, s1)
            }
            Binary::Sub => {
                format!("{}-{}", s0, s1)
            }
            Binary::Index => {
                format!("{}[{}]", s0, s1)
            }
            Binary::Multiply => {
                format!("{}*{}", s0, s1)
            }
        }
    }
}

impl From<&BinaryOperator> for Operator {
    fn from(value: &BinaryOperator) -> Self {
        match value {
            BinaryOperator::Plus => Operator::Add,
            BinaryOperator::Minus => Operator::Minus,
            BinaryOperator::Multiply => Operator::Multiply,
            _ => todo!("unsupported binary operator"),
        }
    }
}

#[derive(Clone)]
pub enum Single {
    Length,
}


impl Sql for Single {
    fn sql(&self) -> String {
        match self {
            Single::Length => String::from("LENGTH"),
        }
    }
}

fn op_len(vm: &mut VM, _: usize) {
    let val = vm.stack.pop().expect("Stack underflow");

    match val {
        Value::Text(t) => vm.stack.push(Value::int(t.0.len() as i64)),
        Value::Array(a) => vm.stack.push(Value::int(a.values.len() as i64)),
        _ => {}
    }
}