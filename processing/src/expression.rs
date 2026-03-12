use crate::expression::Expression::{C, F, L};
use crate::language::Sql;
use crate::operator::Operator;
use sqlparser::ast::{Expr, SelectItem};
use value::{ValType, Value};

#[derive(Clone)]
pub enum Expression {
    F(Field),
    L(Literal),
    C(Call),
}

impl Expression {
    pub fn field(field: Field) -> Self {
        F(field)
    }

    pub fn literal(literal: Literal) -> Self {
        L(literal)
    }

    pub fn call(call: Call) -> Self {
        C(call)
    }
}

impl From<&SelectItem> for Expression {
    fn from(value: &SelectItem) -> Self {
        if let SelectItem::UnnamedExpr(f) = value {
            if let Expr::Identifier(i) = f {
                return Expression::field(Field { name: i.value.clone(), f_type: None });
            } else if let Expr::BinaryOp { left, op, right } = f {
                return Expression::C(Call { operator: Operator::from(op), expressions: vec![Expression::from(left.clone()), Expression::from(right.clone())] });
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
            Expr::Identifier(i) => {
                Expression::field(Field { name: i.value.clone(), f_type: None })
            }
            Expr::Value(v) => {
                match v.value {
                    sqlparser::ast::Value::Number(i, d) => {
                        Expression::literal(Literal {
                            value: Value::float(i.parse().unwrap())
                        })
                    }
                    e => todo!("{:?}", e)
                }
            }
            e => todo!("{:?}", e)
        }
    }
}

impl Sql for Expression {
    fn sql(&self) -> String {
        match self {
            F(f) => {
                f.name.clone()
            }
            L(l) => {
                l.value.to_string()
            }
            C(c) => {
                c.sql()
            }
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


impl Sql for Call {
    fn sql(&self) -> String {
        self.operator.sql(self.expressions.clone())
    }
}
