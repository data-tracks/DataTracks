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
    pub(crate) fn eval(&self, expressions: Vec<Expression>) -> Box<dyn Fn(&Value) -> Value> {
        match self {
            F(f) => f.eval(expressions),
            L(l) => l.eval(),
            C(c) => c.eval(expressions),
        }
    }

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

impl Field {
    pub(crate) fn eval(&self, expressions: Vec<Expression>) -> Box<dyn Fn(&Value) -> Value> {
        todo!()
    }
}

#[derive(Clone)]
pub struct Literal {
    pub(crate) value: Value,
}

impl Literal {
    pub(crate) fn eval(&self) -> Box<dyn Fn(&Value) -> Value> {
        let value = self.value.clone();
        Box::new(move |_| value.clone())
    }
}

#[derive(Clone)]
pub struct Call {
    pub(crate) operator: Operator,
    pub(crate) expressions: Vec<Expression>,
}

impl Call {
    pub(crate) fn eval_bi(&self, expressions: Vec<Expression>) -> Box<dyn Fn(&Value) -> Value> {
        if let Operator::B(b) = &self.operator {
            b.eval(expressions)
        } else {
            panic!()
        }
    }

    fn eval(&self, expressions: Vec<Expression>) -> Box<dyn Fn(&Value) -> Value> {
        self.operator.eval(self.expressions.clone())
    }
}

#[cfg(test)]
mod test {
    use crate::expression::{Call, Expression, Field, Literal};
    use crate::operator::{Binary, Operator};
    use std::vec;
    use value::Value;

    #[test]
    fn test_plus() {
        let func = Call {
            operator: Operator::binary(Binary::Add),
            expressions: vec![
                Expression::literal(Literal {
                    value: Value::int(3),
                }),
                Expression::literal(Literal {
                    value: Value::int(2),
                }),
            ],
        };

        let func = func.eval(vec![]);

        let value = func(&Value::null());
        assert_eq!(value, Value::int(5))
    }

    #[test]
    fn test_plus_sql() {
        let func = Call {
            operator: Operator::binary(Binary::Add),
            expressions: vec![Expression::field(Field {
                name: "var".to_string(),
                f_type: None,
            })],
        };

        if matches!(func.operator, Operator::B(_)) {
            let func = func.eval_bi(vec![]);

            let value = func(&Value::int(3));
        }
    }
}
