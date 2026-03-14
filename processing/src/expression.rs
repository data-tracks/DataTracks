use crate::algebra::Scope;
use crate::language::Sql;
use crate::operator::Operator;
use sqlparser::ast::{Expr, SelectItem};
use std::{cmp, vec};
use serde::Serialize;
use value::{ValType, Value};

#[derive(Clone, Debug, Serialize)]
pub enum Expression {
    Field(String),
    Literal(Value),
    Call {
        operator: Operator,
        expressions: Vec<Expression>,
    },
}

impl Expression {
    pub(crate) fn scope(&self) -> Scope {
        match self {
            Expression::Field(_) => Scope::Tuple,
            Expression::Literal(_) => Scope::Tuple,
            Expression::Call {
                operator,
                expressions,
            } => cmp::max(
                operator.scope(),
                expressions
                    .iter()
                    .map(|e| e.scope())
                    .fold(Scope::Tuple, cmp::max),
            ),
        }
    }
}

impl From<&SelectItem> for Expression {
    fn from(value: &SelectItem) -> Self {
        if let SelectItem::UnnamedExpr(f) = value {
            if let Expr::Identifier(i) = f {
                return Expression::Field(i.value.clone());
            } else if let Expr::BinaryOp { left, op, right } = f {
                return Expression::Call {
                    operator: Operator::from(op),
                    expressions: vec![ Expression::from(left.clone()), Expression::from(right.clone())],
                };
            } else {
                panic!("Expected identifier, found {:?}", f);
            }
        }
        todo!()
    }
}

impl From<Box<Expr>> for Expression {
    fn from(value: Box<Expr>) -> Self {
        match *value {
            Expr::Identifier(i) => Expression::Field(i.value.clone()),
            Expr::Value(v) => match v.value {
                sqlparser::ast::Value::Number(i, _) => {
                    Expression::Literal(Value::float(i.parse().unwrap()))
                }
                e => todo!("{:?}", e),
            },
            e => todo!("{:?}", e),
        }
    }
}

impl Sql for Expression {
    fn sql(&self) -> String {
        match self {
            Expression::Field(f) => f.clone(),
            Expression::Literal(l) => l.to_string(),
            Expression::Call { operator, expressions } => operator.sql(expressions.clone())
        }
    }
}

#[derive(Clone)]
pub struct Field {
    pub(crate) name: String,
    pub(crate) f_type: Option<ValType>,
}

#[derive(Clone)]
pub struct Literal {
    pub(crate) value: Value,
}

#[derive(Clone)]
pub struct Call {
    pub(crate) operator: Operator,
    pub(crate) expressions: Vec<Expression>,
}

