use crate::algebra::Algebra;
use crate::expression::Expression;
use crate::operator::Operator;
use anyhow::anyhow;
use std::collections::HashMap;
use value::Value;

#[derive(Clone, Debug)]
pub enum Op {
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
    Length,

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

#[derive(Clone)]
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
    pub resources: Vec<Box<dyn Iterator<Item = Value> + Send + Sync> >,
}

impl Clone for VM {
    fn clone(&self) -> Self {
        Self{
            stack: self.stack.clone(),
            current_record: self.current_record.clone(),
            constants: self.constants.clone(),
            pc: self.pc.clone(),
            explode_stack: self.explode_stack.clone(),
            resources: vec![],
        }
    }
}



#[derive(Clone)]
pub struct Program {
    instructions: Vec<Op>,
    compiler: Compiler,
    vm: VM,
}

impl From<&Expression> for Program {
    fn from(expression: &Expression) -> Self {
        let mut compiler = Compiler::new();
        let mut instructions = vec![];

        compiler.compile_expr(&expression.clone(), &mut instructions);

        instructions.push(Op::Yield(1));

        Self::new(compiler, instructions)
    }
}

impl From<&Algebra> for Program {
    fn from(algebra: &Algebra) -> Self {
        let mut compiler = Compiler::new();
        let mut instructions = vec![];
        let mut ends = vec![];

        let mut tuples = 1;
        compiler.compile_algebra(&algebra, &mut tuples, &mut instructions, &mut ends);
        instructions.push(Op::Yield(tuples));

        let mut instructions = [instructions, ends].concat();

        // we go back to the iterator
        if let Some(parent_pc) = compiler.loop_stack.last() {
            instructions.push(Op::Jump { target: *parent_pc });
        }

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

    pub fn set_resource<S: AsRef<str>>(
        &mut self,
        name: S,
        iter: impl Iterator<Item = Value> + Send + Sync + 'static,
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

    pub fn reset(&mut self) {
        self.vm.pc = 0;
        self.vm.stack.clear();
        self.vm.current_record.clear();
        self.vm.explode_stack.clear();
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
                    }else {
                        // we end the iterator
                        return None
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
                Op::Length => {
                    let val = self.vm.stack.pop().unwrap();

                    match val {
                        Value::Array(a) => {
                            self.vm.stack.push(Value::int(a.values.len() as i64));
                        }
                        Value::Text(t) => {
                            self.vm.stack.push(Value::int(t.0.len() as i64))
                        }
                        _ => {}
                    }
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
                Op::StoreField(idx) => {
                    let value = self.vm.stack.last().unwrap();
                    self.vm.current_record[*idx] = value.clone()
                }
            }
            self.vm.pc += 1;
        }
        None
    }
}

#[derive(Clone, Debug)]
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
            Expression::Literal(value) => {
                let idx = self.constants.len();
                self.constants.push(value.clone());
                out.push(Op::PushConst(idx));
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
        }
    }

    pub fn compile_op(op: &Operator) -> Op {
        match op {
            Operator::Add => Op::Add,
            Operator::Gt => Op::Greater,
            Operator::Index => Op::Index,
            Operator::Minus => Op::Minus,
            Operator::Multiply => Op::Multiply,
            Operator::Explode => Op::InitExplode(0),
        }
    }

    pub fn compile_algebra(
        &mut self,
        algebra: &Algebra,
        tuples: &mut usize,
        ops: &mut Vec<Op>,
        ends: &mut Vec<Op>,
    ) {
        match algebra {
            Algebra::Scan { source } => {
                let start_pc = ops.len();

                let slot = self.resource_map.len();
                let slot = *self
                    .resource_map
                    .entry(source.to_string())
                    .or_insert_with(|| slot);

                ops.push(Op::NextTuple { resource_id: slot }); // Start the loop
                self.loop_stack.push(start_pc);
            }
            Algebra::F(filter) => {
                // 1. First, compile the source (Scan)
                self.compile_algebra(&filter.input, tuples, ops, ends);

                // 2. Compile the condition (e.g., x > 10)
                self.compile_expr(&filter.predicate, ops);

                // 3. Jump to the start if condition is false (skips Yield)
                let start_pc = *self.loop_stack.last().unwrap();
                ops.push(Op::JumpIfFalse { target: start_pc });
            }
            Algebra::P(project) => {
                // 1. Compile input (e.g., Scan)
                self.compile_algebra(&project.input, tuples, ops, ends);

                for (_, expr) in &project.expressions {
                    self.compile_expr(expr, ops);
                }

                *tuples = project.expressions.len();
            }
            Algebra::T(_) => {
                let start_pc = ops.len();

                let slot = self.resource_map.len();
                let slot = *self
                    .resource_map
                    .entry("$$source".to_string())
                    .or_insert_with(|| slot);

                ops.push(Op::NextTuple { resource_id: slot }); // Start the loop
                self.loop_stack.push(start_pc);
                //panic!("T algebra not yet implemented");
                ops.push(Op::Yield(1));
            }
            Algebra::U(unwind) => {
                self.compile_algebra(&unwind.input, tuples, ops, ends);
                // --- LOOP SETUP ---

                ops.push(self.compile_field(&unwind.key));

                // C. Instruction to move the array from Stack -> VM.explode_stack
                let loop_start_pc = ops.len() + 1;

                ops.push(match unwind.func {
                    Operator::Explode => Op::InitExplode(loop_start_pc),
                    _ => panic!("Unwind algebra not yet implemented"),
                });

                self.loop_stack.push(loop_start_pc);

                // --- LOOP BODY ---
                // D. Load current element of the latest explode onto the stack
                ops.push(Op::LoadExplodeElement);


                let idx = self.field_map.get(&unwind.key).unwrap();
                ops.push(Op::StoreField(idx.clone()));

                // G. Advance the explode loop
                // If has next: jumps to loop_start_pc + 1 (LoadExplodeElement)
                // If done: pops explode_stack and continues to next instruction
                ends.push(Op::NextOrPop);

                self.loop_stack.pop(); // Done with this level
            }
            Algebra::C(_) => todo!(),
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
    use crate::operator::Operator;

    #[test]
    fn test_vm_execution_add() {
        // Simulate: price + 10
        let expr = Expression::Call {
            operator: Operator::Add,
            expressions: vec![
                Expression::Field("price".to_string()),
                Expression::Literal(Value::int(10)),
            ],
        };

        let mut program = Program::from(expr);
        program.set_record("price", Value::int(100)).unwrap();

        assert_eq!(program.next().unwrap(), Value::int(110));
    }

    #[test]
    fn test_vm_execution_multiple() {
        // Simulate: price + 10
        let mut program = Program::from(Algebra::project(
            Algebra::scan("test"),
            [
                (
                    "name".to_string(),
                    Expression::Call {
                        operator: Operator::Add,
                        expressions: vec![
                            Expression::Field("name".to_string()),
                            Expression::Literal(Value::int(10)),
                        ],
                    },
                ),
                (
                    "name1".to_string(),
                    Expression::Field("name".to_string()),
                ),
            ],
        ));

        program.set_resource("test", [Value::int(100)].into_iter()).unwrap();

        assert_eq!(program.next().unwrap(), Value::array([Value::int(110), Value::int(100)]));
    }

    #[test]
    fn test_vm_execution_explode() {
        // Simulate: explode
        let mut program = Program::from(Algebra::unwind(
            Algebra::scan("test"),
            "name",
            Operator::Explode,
        ));

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
    fn test_vm_execution_explode_nested() {
        // Simulate: explode
        let mut program = Program::from(Algebra::project(
            Algebra::unwind(Algebra::scan("test"), "name", Operator::Explode),
            [(
                "name".to_string(),
                Expression::Call {
                    operator: Operator::Add,
                    expressions: vec![
                        Expression::Field("name".to_string()),
                        Expression::Literal(Value::text("test")),
                    ],
                },
            )],
        ));

        program
            .set_resource("test", [Value::text("David")].into_iter())
            .unwrap();

        assert_eq!(program.next().unwrap(), Value::text("Dtest"));
        assert_eq!(program.next().unwrap(), Value::text("atest"));
        assert_eq!(program.next().unwrap(), Value::text("vtest"));
        assert_eq!(program.next().unwrap(), Value::text("itest"));
        assert_eq!(program.next().unwrap(), Value::text("dtest"));
    }

    #[test]
    fn test_vm_execution_array() {
        // Simulate: array[0] + array[1]
        let expr = Expression::Call {
            operator: Operator::Add,
            expressions: vec![
                Expression::Call {
                    operator: Operator::Index,
                    expressions: vec![
                        Expression::Field("array".to_string()),
                        Expression::Literal(Value::int(0)),
                    ],
                },
                Expression::Call {
                    operator: Operator::Index,
                    expressions: vec![
                        Expression::Field("array".to_string()),
                        Expression::Literal(Value::int(1)),
                    ],
                },
            ],
        };

        let mut program = Program::from(expr);
        program.set_record("array", Value::text("text")).unwrap();

        assert_eq!(program.next().unwrap(), Value::text("te"));
    }
}
