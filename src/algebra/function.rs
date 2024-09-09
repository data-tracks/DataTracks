use crate::algebra::Operator;
use crate::value::Value;

#[derive(Debug)]
pub enum Function{
    Literal(LiteralOperator(Value)),
    NamedRef(NamedRefOperator(String)),
    IndexedRef(IndexedRefOperator(u64)),
    Operation(OperationFunction(Operator,Vec<Function>))
}


#[derive(Debug)]
pub struct LiteralOperator{
    literal: Value
}

#[derive(Debug)]
pub struct NamedRefOperator{
    name: String
}

#[derive(Debug)]
pub struct IndexedRefOperator{
    index: u64
}

#[derive(Debug)]
pub struct OperationFunction{
    op: Operator,
    operands: Vec<Function>
}