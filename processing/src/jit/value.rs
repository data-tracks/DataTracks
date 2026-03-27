// Newtype for type-safe variable referencing
#[derive(Debug, Clone, Copy)]
pub struct VarId(pub u32);

pub enum Expr {
    Col(VarId),
    Literal(i64),
    Add(Box<Expr>, Box<Expr>),
}

pub enum LogicalOp {
    Scan { table_id: u32 },
    Filter { input: Box<LogicalOp>, condition: Expr },
    Project { input: Box<LogicalOp>, expressions: Vec<Expr> },
}