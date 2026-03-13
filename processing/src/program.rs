use crate::algebra::{Algebra, Op};
use crate::expression::{Call, Expression};
use crate::operator::Operator;
use anyhow::anyhow;
use std::collections::HashMap;
use value::Value;

pub struct ExplodeState {
    pub array: Vec<Value>,
    pub index: usize,
    pub loop_pc: usize, // Where to jump back to for the next element
}

pub struct VM {
    pub(crate) stack: Vec<Value>,
    current_record: Vec<Value>,
    constants: Vec<Value>, // The "Pool" for literals
    pub pc: usize,         // Program Counter
    explode_stack: Vec<ExplodeState>,
    pub resources: Vec<Box<dyn Iterator<Item = Value>>>,
}

pub struct Program {
    instructions: Vec<Op>,
    compiler: Compiler,
    vm: VM,
}

impl From<Expression> for Program {
    fn from(expression: Expression) -> Self {
        let mut compiler = Compiler::new();
        let mut instructions = vec![];
        compiler.compile_expr(&expression.clone(), &mut instructions);

        instructions.push(Op::Yield(1));

        Self::new(compiler, instructions)
    }
}

impl From<Algebra> for Program {
    fn from(algebra: Algebra) -> Self {
        let mut compiler = Compiler::new();
        let mut instructions = vec![];
        compiler.compile_algebra(&algebra, &mut instructions);

        Self::new(compiler, instructions)
    }
}

impl Program {
    pub fn new(compiler: Compiler, instructions: Vec<Op>) -> Program {
        let vm = VM {
            stack: Vec::with_capacity(16),
            resources: vec![],
            current_record: vec![],
            constants: compiler.constants.clone(),
            pc: 0,
            explode_stack: vec![],
        };

        Self {
            instructions,
            compiler,
            vm,
        }
    }

    pub(crate) fn set_resource<S: AsRef<str>>(
        &mut self,
        name: S,
        iter: impl Iterator<Item = Value> + 'static,
    ) -> anyhow::Result<()> {
        let index = self
            .compiler
            .resource_map
            .get(name.as_ref())
            .ok_or(anyhow!("No named resource in compiler"))?;
        self.vm.resources.insert(*index, Box::new(iter));
        Ok(())
    }

    pub(crate) fn set_record(&mut self, name: &str, value: Value) -> anyhow::Result<()> {
        let index = self
            .compiler
            .field_map
            .get(name)
            .ok_or(anyhow!("No named field in compiler"))?;
        self.vm.current_record.insert(*index, value);
        Ok(())
    }
}

impl Iterator for Program {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        while self.vm.pc < self.instructions.len() {
            let instr = &self.instructions[self.vm.pc];

            match instr {
                Op::PushConst(idx) => {
                    self.vm.stack.push(self.vm.constants[*idx].clone());
                }
                Op::LoadField(idx) => {
                    self.vm.stack.push(self.vm.current_record[*idx].clone());
                }
                Op::Add => {
                    let r = self.vm.stack.pop().unwrap();
                    let l = self.vm.stack.pop().unwrap();
                    self.vm.stack.push(&l + &r);
                }
                Op::JumpIfFalse { target } => {
                    if !self.vm.stack.pop().unwrap().as_bool().unwrap().0 {
                        self.vm.pc = *target;
                        continue; // Skip the standard pc += 1
                    }
                }
                Op::Yield(amount) => {
                    let mut row = Vec::with_capacity(*amount);
                    for _ in 0..*amount {
                        row.push(self.vm.stack.pop().expect("Stack underflow at yield"));
                    }
                    row.reverse();

                    self.vm.pc += 1; // Move past Yield for the next call
                    return if amount == &1 {
                        row.pop()
                    } else {
                        Some(Value::array(row))
                    };
                }
                Op::Equal => {
                    let r = self.vm.stack.pop().unwrap();
                    let l = self.vm.stack.pop().unwrap();
                    self.vm.stack.push(&l + &r);
                }
                Op::NextTuple { resource_id } => {
                    if let Some(resource) = self.vm.resources.get_mut(*resource_id)
                        && let Some(value) = resource.next()
                    {
                        self.vm.current_record.push(value)
                    }
                }
                Op::Jump { target } => {
                    self.vm.pc = *target;
                    continue; // Skip the standard pc += 1
                }
                Op::Greater => {
                    let r = self.vm.stack.pop().unwrap();
                    let l = self.vm.stack.pop().unwrap();
                    self.vm.stack.push(Value::bool(l > r));
                }

                Op::Index => {
                    let index = self
                        .vm
                        .stack
                        .pop()
                        .expect("Stack underflow")
                        .as_int()
                        .unwrap()
                        .0 as usize;
                    let array = self.vm.stack.pop().expect("Stack underflow");
                    if let Value::Array(a) = array {
                        self.vm.stack.push(a.values[index].clone());
                    } else if let Value::Text(t) = array {
                        self.vm.stack.push(Value::text(&t.0[index..index + 1]))
                    }
                }
                Op::Minus => {
                    let r = self.vm.stack.pop().unwrap();
                    let l = self.vm.stack.pop().unwrap();
                    self.vm.stack.push(&l - &r);
                }
                Op::Multiply => {
                    let r = self.vm.stack.pop().unwrap();
                    let l = self.vm.stack.pop().unwrap();
                    self.vm.stack.push(&l * &r);
                }
                Op::NextOrPop => {
                    if let Some(state) = self.vm.explode_stack.last_mut() {
                        state.index += 1;
                        if state.index < state.array.len() {
                            // Keep looping this array
                            self.vm.pc = state.loop_pc;
                            continue;
                        } else {
                            // This array is done!
                            self.vm.explode_stack.pop();
                        }
                    }
                }
                Op::LoadExplodeElement => {
                    let state = self.vm.explode_stack.last().unwrap();
                    let val = state.array[state.index].clone();
                    self.vm.stack.push(val);
                }
                Op::InitExplode(start_pc) => {
                    let array_val = self.vm.stack.pop().unwrap();
                    if let Value::Array(arr) = array_val {
                        self.vm.explode_stack.push(ExplodeState {
                            array: arr.values,
                            index: 0,
                            loop_pc: *start_pc,
                        });
                    } else if let Value::Text(text) = array_val {
                        self.vm.explode_stack.push(ExplodeState {
                            array: text
                                .0
                                .chars()
                                .map(|c| Value::text(&c.to_string()))
                                .collect(),
                            index: 0,
                            loop_pc: *start_pc,
                        });
                    }
                }
            }
            self.vm.pc += 1;
        }
        panic!()
    }
}

pub struct Compiler {
    pub field_map: HashMap<String, usize>,
    pub resource_map: HashMap<String, usize>,
    pub constants: Vec<Value>,
    pub next_slot: usize,
    pub loop_stack: Vec<usize>,
}

impl Compiler {
    pub fn new() -> Self {
        Self {
            field_map: HashMap::new(),
            resource_map: HashMap::new(),
            constants: Vec::new(),
            loop_stack: Vec::new(),
            next_slot: 0,
        }
    }

    pub fn compile_expr(&mut self, expr: &Expression, out: &mut Vec<Op>) {
        match expr {
            Expression::L(lit) => {
                let idx = self.constants.len();
                self.constants.push(lit.value.clone());
                out.push(Op::PushConst(idx));
            }
            Expression::F(field) => {
                let op = self.compile_field(&field.name);
                out.push(op);
            }
            Expression::C(call) => {
                for e in &call.expressions {
                    self.compile_expr(e, out);
                }
                // Map your operators to the enum
                match call.operator {
                    Operator::Add => out.push(Op::Add),
                    Operator::Gt => out.push(Op::Greater),
                    Operator::Index => out.push(Op::Index),
                    Operator::Minus => out.push(Op::Minus),
                    Operator::Multiply => out.push(Op::Multiply),
                    Operator::Explode => out.push(Op::InitExplode(0)),
                }
            }
        }
    }

    pub fn compile_algebra(&mut self, algebra: &Algebra, out: &mut Vec<Op>) {
        match algebra {
            Algebra::S(s) => {
                let start_pc = out.len();

                let slot = self.resource_map.len();
                let slot = *self
                    .resource_map
                    .entry(s.resource.to_string())
                    .or_insert_with(|| slot);

                out.push(Op::NextTuple { resource_id: slot }); // Start the loop
                self.loop_stack.push(start_pc);
            }
            Algebra::F(filter) => {
                // 1. First, compile the source (Scan)
                self.compile_algebra(&filter.input, out);

                // 2. Compile the condition (e.g., x > 10)
                self.compile_expr(&filter.predicate, out);

                // 3. Jump to the start if condition is false (skips Yield)
                let start_pc = *self.loop_stack.last().unwrap();
                out.push(Op::JumpIfFalse { target: start_pc });
            }
            Algebra::P(project) => {
                // 1. Compile input (e.g., Scan)
                self.compile_algebra(&project.input, out);

                // 2. Identify the explode
                let explode_idx = project.expressions.iter().position(|e| {
                    if let Expression::C(Call { operator, .. }) = e {
                        matches!(operator, Operator::Explode)
                    } else {
                        false
                    }
                });

                if let Some(idx) = explode_idx {
                    // --- LOOP SETUP ---
                    // A. Push "stable" fields (before the explode) to the stack
                    for i in 0..idx {
                        self.compile_expr(&project.expressions[i], out);
                    }

                    // B. Compile the array expression
                    if let Expression::C(c) = &project.expressions[idx] {
                        self.compile_expr(&c.expressions[0], out);
                    }

                    // C. Instruction to move the array from Stack -> VM.explode_stack
                    let loop_start_pc = out.len();
                    out.push(Op::InitExplode(loop_start_pc + 1));

                    // --- LOOP BODY ---
                    // D. Load current element of the latest explode onto the stack
                    out.push(Op::LoadExplodeElement);

                    // E. Compile "suffix" fields (after the explode)
                    for i in (idx + 1)..project.expressions.len() {
                        self.compile_expr(&project.expressions[i], out);
                    }

                    // F. Yield the row
                    out.push(Op::Yield(project.expressions.len()));

                    // G. Advance the explode loop
                    // If has next: jumps to loop_start_pc + 1 (LoadExplodeElement)
                    // If done: pops explode_stack and continues to next instruction
                    out.push(Op::NextOrPop);

                    // H. After the explode is totally done, we need to loop the SCAN
                    let scan_pc = *self.loop_stack.first().unwrap();
                    out.push(Op::Jump { target: scan_pc });
                } else {
                    // --- STANDARD PROJECTION ---
                    for expr in &project.expressions {
                        self.compile_expr(expr, out);
                    }
                    out.push(Op::Yield(project.expressions.len()));
                    let jump_target = *self.loop_stack.last().unwrap();
                    out.push(Op::Jump {
                        target: jump_target,
                    });
                }
            }
            Algebra::T(_) => {
                panic!("T algebra not yet implemented");
            }
            Algebra::C(_) | Algebra::U(_) => todo!(),
        }
    }

    fn compile_field(&mut self, name: &str) -> Op {
        let slot = *self.field_map.entry(name.to_string()).or_insert_with(|| {
            let id = self.next_slot;
            self.next_slot += 1;
            id
        });

        Op::LoadField(slot)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::expression::{Call, Field, Literal};
    use crate::operator::Operator;

    #[test]
    fn test_vm_execution_add() {
        // Simulate: price + 10
        let expr = Expression::C(Call {
            operator: Operator::Add,
            expressions: vec![
                Expression::F(Field {
                    name: "price".into(),
                    f_type: None,
                }),
                Expression::L(Literal {
                    value: Value::int(10),
                }),
            ],
        });

        let mut program = Program::from(expr);
        program.set_record("price", Value::int(100)).unwrap();

        assert_eq!(program.next().unwrap(), Value::int(110));
    }

    #[test]
    fn test_vm_execution_explode() {
        // Simulate: explode
        let expr = Expression::C(Call {
            operator: Operator::Explode,
            expressions: vec![Expression::F(Field {
                name: "name".into(),
                f_type: None,
            })],
        });

        let mut program = Program::from(Algebra::project(Algebra::scan("test"), expr));

        program
            .set_resource("test", [Value::text("David")].into_iter())
            .unwrap();

        assert_eq!(program.next().unwrap(), Value::text("D"));
        assert_eq!(program.next().unwrap(), Value::text("a"));
        assert_eq!(program.next().unwrap(), Value::text("v"));
        assert_eq!(program.next().unwrap(), Value::text("i"));
        assert_eq!(program.next().unwrap(), Value::text("d"));
    }

    #[test]
    fn test_vm_execution_array() {
        // Simulate: array[0] + array[1]
        let expr = Expression::C(Call {
            operator: Operator::Add,
            expressions: vec![
                Expression::C(Call {
                    operator: Operator::Index,
                    expressions: vec![
                        Expression::F(Field {
                            name: "array".into(),
                            f_type: None,
                        }),
                        Expression::L(Literal {
                            value: Value::int(0),
                        }),
                    ],
                }),
                Expression::C(Call {
                    operator: Operator::Index,
                    expressions: vec![
                        Expression::F(Field {
                            name: "array".into(),
                            f_type: None,
                        }),
                        Expression::L(Literal {
                            value: Value::int(1),
                        }),
                    ],
                }),
            ],
        });

        let mut program = Program::from(expr);
        program.set_record("array", Value::text("text")).unwrap();

        assert_eq!(program.next().unwrap(), Value::text("te"));
    }
}
