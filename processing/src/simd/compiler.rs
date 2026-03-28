use crate::instruction::Instruction;
use crate::{Algebra, Scan, Schema};
use std::collections::HashMap;
use std::fmt::Debug;
use value::{ValType, Value};
use crate::expression::Expression;
use crate::operator::Operator;

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
        ops: &mut Vec<Instruction>,
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

            }
            Algebra::Project(project) => {
                // 1. Compile input (e.g., Scan)
                self.compile_algebra(&project.input, ops);

                for (_, expr) in &project.expressions {
                    self.compile_expr(expr, ops);
                }

            }
            Algebra::Filter(_) => {}
            Algebra::Collect(_) => {}
            Algebra::Unwind(_) => {}
            Algebra::Todo(_) => {}
        }
    }

    fn compile_field(&mut self, name: &String) -> Instruction {
        let slot = if let Schema::Fixed(f) = &mut self.current_schema {
            f.insert_full(name.to_string(), ValType::Any).0
        } else {
            Schema::fixed([(name.to_string(), ValType::Any)]);
            0
        };

        Instruction::LoadField(slot)
    }
}
