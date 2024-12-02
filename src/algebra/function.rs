use crate::algebra::algebra::BoxedValueLoader;
use crate::algebra::operator::{AggOp, InputOp, LiteralOp, NameOp, Op};
use crate::algebra::Op::Tuple;
use crate::algebra::TupleOp::{Combine, Context, Input, Literal, Name};
use crate::algebra::{BoxedValueHandler, ContextOp, TupleOp};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::optimize::Cost;
use crate::processing::Layout;
use crate::value::Value;
use std::collections::HashMap;

pub trait Replaceable {
    fn replace(
        &mut self,
        replace: fn(&mut Operator) -> Vec<(AggOp, Vec<Operator>)>,
    ) -> Vec<(AggOp, Vec<Operator>)>;
}

pub trait Implementable<Implementation> {
    fn implement(&self) -> Result<Implementation, ()>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Operator {
    pub op: Op,
    pub operands: Vec<Operator>,
}

impl Operator {
    pub fn new(op: Op, operands: Vec<Operator>) -> Operator {
        Operator { op, operands }
    }

    pub fn name(name: &str, operands: Vec<Operator>) -> Operator {
        Operator {
            op: Tuple(Name(NameOp::new(name.to_string()))),
            operands,
        }
    }

    pub(crate) fn calc_cost(&self) -> Cost {
        match &self.op {
            Op::Agg(a) => match a {
                AggOp::Count => Cost::new(1),
                AggOp::Sum => Cost::new(1),
                AggOp::Avg => Cost::new(1),
            },
            Tuple(t) => match t {
                TupleOp::Plus
                | TupleOp::Minus
                | TupleOp::Multiplication
                | TupleOp::Division
                | TupleOp::Equal
                | TupleOp::KeyValue(_) => {
                    self.operands[0].calc_cost() + self.operands[1].calc_cost()
                }
                Combine | TupleOp::And | TupleOp::Or | TupleOp::Doc => self
                    .operands
                    .iter()
                    .map(|o| o.calc_cost())
                    .fold(Cost::new(0), |a, b| a + b),
                TupleOp::Not => self.operands[0].calc_cost(),
                Input(_) => Cost::new(1),
                Name(_) => Cost::new(1),
                TupleOp::Index(_) => Cost::new(1),
                Literal(_) => Cost::new(1),
                Context(_) => Cost::new(1),
            },
            Op::Collection(_) => {
                self.operands.iter().map(|o| o.calc_cost()).reduce(|a, b| a + b).unwrap_or(Cost::new(0))
            }
        }
    }

    pub fn context(name: String) -> Operator {
        Operator {
            op: Tuple(Context(ContextOp::new(name))),
            operands: vec![],
        }
    }

    // $0.1 -> 1
    pub fn index(index: usize, operands: Vec<Operator>) -> Self {
        Operator {
            op: Op::index(index),
            operands,
        }
    }
    // $0 -> 0
    pub fn input() -> Self {
        Operator {
            op: Tuple(Input(InputOp {})),
            operands: vec![],
        }
    }

    pub fn literal(literal: Value) -> Self {
        Operator {
            op: Tuple(Literal(LiteralOp { literal })),
            operands: vec![],
        }
    }

    pub(crate) fn combine(values: Vec<Operator>) -> Self {
        Operator {
            op: Tuple(Combine),
            operands: values,
        }
    }
}

impl OutputDerivable for Operator {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        Some(
            self.op.derive_output_layout(
                self.operands
                    .iter()
                    .cloned()
                    .map(|o| o.derive_output_layout(inputs.clone()).unwrap())
                    .collect(),
                inputs,
            ),
        )
    }
}

impl InputDerivable for Operator {
    fn derive_input_layout(&self) -> Option<Layout> {
        Some(
            self.op.derive_input_layout(
                self.operands
                    .iter()
                    .cloned()
                    .map(|o| o.derive_input_layout().unwrap_or_default())
                    .collect(),
            ),
        )
    }
}

impl Replaceable for Operator {
    fn replace(
        &mut self,
        replace: fn(&mut Operator) -> Vec<(AggOp, Vec<Operator>)>,
    ) -> Vec<(AggOp, Vec<Operator>)> {
        match &self.op {
            Op::Agg(_) => replace(self),
            Tuple(_) | Op::Collection(_) => self
                .operands
                .iter_mut()
                .flat_map(|o| o.replace(replace))
                .collect(),
        }
    }
}

impl Implementable<BoxedValueHandler> for Operator {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        match &self.op {
            Op::Agg(_) => Err(()),
            Tuple(t) => Ok(t.implement(self.operands.clone())),
            Op::Collection(_) => Err(())
        }
    }
}

impl Implementable<BoxedValueLoader> for Operator {
    fn implement(&self) -> Result<BoxedValueLoader, ()> {
        match &self.op {
            Op::Agg(a) => a.implement(),
            Tuple(_) => Err(()),
            Op::Collection(_) => Err(())
        }
    }
}
