#[derive(Clone, Debug)]
pub enum Instruction {
    // Scalar Ops
    LoadField(usize), // load value from record
    StoreField(usize),
    PushConst(usize),
    Add,
    Greater,
    Equal,
    Index,
    Minus,
    Multiply,
    Divide,
    Length,

    // Flatten
    Flatten,

    // Explode
    NextOrPop,
    LoadExplodeElement,
    InitExplode(usize),

    // Ops (The Algebra)
    NextTuple { resource_id: usize }, // holds the "raw" data so that multiple different expressions (filters, math, etc.) can all look at the same row simultaneously without fighting over the stack.
    JumpIfFalse { target: usize },    // jump if top is false
    Jump { target: usize },

    // The "Materialize" Op
    // arg = how many items to pop from stack to form the result row
    Yield(usize),
}
