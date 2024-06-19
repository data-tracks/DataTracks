use std::collections::HashMap;

use crate::value::Value;

pub(crate) struct Block {
    blocks: Vec<i64>,
    buffer: HashMap<i64, Vec<Value>>,
}


impl Block {
    pub(crate) fn new(blocks: Vec<i64>) -> Self {
        Block { blocks, buffer: HashMap::new() }
    }
}