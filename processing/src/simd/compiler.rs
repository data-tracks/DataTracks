use crate::instruction::Instruction;
use crate::{Algebra, Scan, Schema};
use std::collections::HashMap;
use std::fmt::Debug;
use value::Value;

#[derive(Clone, Debug)]
pub struct Compiler {
    //pub field_map: HashMap<String, usize>,
    pub resource_map: HashMap<String, usize>,
    pub constants: Vec<Value>,
    pub loop_stack: Vec<usize>,
    pub current_schema: Schema,
}

impl Default for Compiler {
    fn default() -> Self {
        Self::new()
    }
}

impl Compiler {
    pub fn new() -> Self {
        Self {
            //field_map: HashMap::new(),
            resource_map: HashMap::new(),
            constants: Vec::new(),
            loop_stack: Vec::new(),
            current_schema: Schema::Dynamic,
        }
    }

    pub fn compile_algebra(
        &mut self,
        algebra: &Algebra,
        tuples: &mut usize,
        ops: &mut Vec<Instruction>,
        ends: &mut Vec<Instruction>,
    ) {
        match algebra {
            Algebra::Scan(Scan { source, schema }) => {
                let start_pc = ops.len();

                let slot = self.resource_map.len();
                let slot = *self
                    .resource_map
                    .entry(source.to_string())
                    .or_insert_with(|| slot);

                self.current_schema = schema.clone();

                ops.push(Instruction::NextTuple { resource_id: slot }); // Start the loop
                self.loop_stack.push(start_pc);

                // we do not only have one field aka doc, but named fields
                if let Schema::Fixed(_) = &self.current_schema {
                    ops.push(Instruction::Flatten);
                }
            }
            Algebra::Project(_) => {}
            Algebra::Filter(_) => {}
            Algebra::Collect(_) => {}
            Algebra::Unwind(_) => {}
            Algebra::Todo(_) => {}
        }
    }
}
