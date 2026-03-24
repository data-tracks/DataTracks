use crate::expression::Expression;
use crate::instruction::Instruction;
use crate::operator::Operator;
use crate::{Algebra, Scan, Schema};
use std::collections::HashMap;
use value::{ValType, Value};

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

    pub fn compile_expr(&mut self, expr: &Expression, out: &mut Vec<Instruction>) {
        match expr {
            Expression::Literal(value) => {
                let idx = self.constants.len();
                self.constants.push(value.clone());
                out.push(Instruction::PushConst(idx));
            }
            Expression::Field(name) => {
                let op = self.compile_field(name);
                out.push(op);
            }
            Expression::Call {
                operator,
                expressions,
            } => {
                for e in expressions {
                    self.compile_expr(e, out);
                }
                // Map the operators to the enum
                out.push(Self::compile_op(operator))
            }
            Expression::Exclude(_) => {
                todo!()
            }
        }
    }

    pub fn compile_op(op: &Operator) -> Instruction {
        match op {
            Operator::Add => Instruction::Add,
            Operator::Gt => Instruction::Greater,
            Operator::Index => Instruction::Index,
            Operator::Minus => Instruction::Minus,
            Operator::Multiply => Instruction::Multiply,
            Operator::Explode => Instruction::InitExplode(0),
            Operator::Equal => Instruction::Equal,
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
            Algebra::Filter(filter) => {
                // 1. First, compile the source (Scan)
                self.compile_algebra(&filter.input, tuples, ops, ends);

                // 2. Compile the condition (e.g., x > 10)
                self.compile_expr(&filter.predicate, ops);

                // 3. Jump to the start if condition is false (skips Yield)
                let start_pc = *self.loop_stack.last().unwrap();
                ops.push(Instruction::JumpIfFalse { target: start_pc });
            }
            Algebra::Project(project) => {
                // 1. Compile input (e.g., Scan)
                self.compile_algebra(&project.input, tuples, ops, ends);

                for (_, expr) in &project.expressions {
                    self.compile_expr(expr, ops);
                }

                *tuples = project.expressions.len();
            }
            Algebra::Todo(_) => {
                let start_pc = ops.len();

                let slot = self.resource_map.len();
                let slot = *self
                    .resource_map
                    .entry("$$source".to_string())
                    .or_insert_with(|| slot);

                ops.push(Instruction::NextTuple { resource_id: slot }); // Start the loop
                self.loop_stack.push(start_pc);
                //panic!("T algebra not yet implemented");
                ops.push(Instruction::Yield(1));
            }
            Algebra::Unwind(unwind) => {
                self.compile_algebra(&unwind.input, tuples, ops, ends);
                // --- LOOP SETUP ---

                ops.push(self.compile_field(&unwind.key));

                // C. Instruction to move the array from Stack -> VM.explode_stack
                let loop_start_pc = ops.len() + 1;

                ops.push(match unwind.func {
                    Operator::Explode => Instruction::InitExplode(loop_start_pc),
                    _ => panic!("Unwind algebra operator not yet implemented"),
                });

                self.loop_stack.push(loop_start_pc);

                // --- LOOP BODY ---
                // D. Load current element of the latest explode onto the stack
                ops.push(Instruction::LoadExplodeElement);

                let idx = self.current_schema.get(&unwind.key).unwrap();
                ops.push(Instruction::StoreField(idx));

                // G. Advance the explode loop
                // If has next: jumps to loop_start_pc + 1 (LoadExplodeElement)
                // If done: pops explode_stack and continues to next instruction
                ends.push(Instruction::NextOrPop);

                self.loop_stack.pop(); // Done with this level
            }
            Algebra::Collect(_) => todo!(),
        }
    }

    fn compile_field(&mut self, name: &str) -> Instruction {
        let slot = if let Schema::Fixed(f) = &mut self.current_schema {
            f.insert(name.to_string(), ValType::Any);
            f.len() - 1
        } else {
            Schema::fixed([(name.to_string(), ValType::Any)]);
            0
        };

        Instruction::LoadField(slot)
    }
}
