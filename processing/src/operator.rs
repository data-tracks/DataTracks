use crate::algebra::Scope;
use crate::expression::Expression;
use crate::language::Sql;
use serde::Serialize;
use sqlparser::ast::BinaryOperator;

#[derive(Clone, Debug, Serialize)]
pub enum Operator {
    Add,
    Equal,
    Minus,
    Multiply,
    Gt,
    Index,
    Explode,
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
            Operator::Equal => format!("{} = {}", expressions[0].sql(), expressions[1].sql()),
        }
    }

    pub(crate) fn scope(&self) -> Scope {
        match self {
            Operator::Add => Scope::Tuple,
            Operator::Minus => Scope::Tuple,
            Operator::Multiply => Scope::Tuple,
            Operator::Gt => Scope::Tuple,
            Operator::Index => Scope::Tuple,
            Operator::Explode => Scope::Tuple,
            Operator::Equal => Scope::Tuple,
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
