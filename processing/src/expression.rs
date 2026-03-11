use crate::expression::Expression::{C, F, L};
use crate::operator::Operator;
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

