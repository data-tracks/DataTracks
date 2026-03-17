use crate::algebra::Scope;
use crate::language::Sql;
use crate::operator::Operator;
use mongodb::bson::Bson;
use serde::Serialize;
use sqlparser::ast::{Expr, SelectItem};
use std::{cmp, vec};
use value::Value;

#[derive(Clone, Debug, Serialize)]
pub enum Expression {
    Field(String),
    Literal(Value),
    Exclude(String),
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
            Expression::Exclude(_) => Scope::Tuple
        }
    }

    pub(crate) fn field(name: &str) -> Self {
        Self::Field(name.to_string())
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
                    expressions: vec![
                        Expression::from(left.clone()),
                        Expression::from(right.clone()),
                    ],
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

impl From<(&str, &Bson)> for Expression {
    fn from(value: (&str, &Bson)) -> Self {
        if let Some(num) = value.1.as_i64() {
            if num == 1 {
                Expression::Field(value.0.to_string())
            } else {
                Expression::Exclude(value.0.to_string())
            }
        }else if let Some (field) = value.1.as_str() {
            Expression::Field(field.to_string())
        }else {
            Expression::from(value.1)
        }
    }
}

impl From<&Bson> for Expression {
    fn from(value: &Bson) -> Self {
        todo!()
    }
}

impl Sql for Expression {
    fn sql(&self) -> String {
        match self {
            Expression::Field(f) => f.clone(),
            Expression::Literal(l) => l.to_string(),
            Expression::Call {
                operator,
                expressions,
            } => operator.sql(expressions.clone()),
            Expression::Exclude(_) => unreachable!()
        }
    }
}
