use crate::Schema;
use crate::algebra::Algebra;
use crate::expression::Expression;
use crate::instruction::Instruction;
use crate::tuple::compiler::Compiler;
use crate::tuple::vm::VM;
use anyhow::anyhow;
use value::Value;

#[derive(Clone)]
pub struct ExplodeState {
    pub array: Vec<Value>,
    pub index: usize,
    pub loop_pc: usize, // Where to jump back to for the next element
}

#[derive(Clone)]
pub struct Program {
    instructions: Vec<Instruction>,
    compiler: Compiler,
    vm: VM,
}

impl From<&Expression> for Program {
    fn from(expression: &Expression) -> Self {
        let mut compiler = Compiler::new();
        let mut instructions = vec![];

        compiler.compile_expr(&expression.clone(), &mut instructions);

        instructions.push(Instruction::Yield(1));

        Self::new(compiler, instructions)
    }
}

impl From<&Algebra> for Program {
    fn from(algebra: &Algebra) -> Self {
        let mut compiler = Compiler::new();
        let mut instructions = vec![];
        let mut ends = vec![];

        let mut tuples = 1;
        compiler.compile_algebra(algebra, &mut tuples, &mut instructions, &mut ends);
        instructions.push(Instruction::Yield(tuples));

        let mut instructions = [instructions, ends].concat();

        // we go back to the iterator
        if let Some(parent_pc) = compiler.loop_stack.last() {
            instructions.push(Instruction::Jump { target: *parent_pc });
        }

        Self::new(compiler, instructions)
    }
}

impl Program {
    pub fn new(compiler: Compiler, instructions: Vec<Instruction>) -> Program {
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

    #[cfg(test)]
    pub(crate) fn set_record(&mut self, name: &str, value: Value) -> anyhow::Result<()> {
        let index = self
            .compiler
            .current_schema
            .get(name)
            .ok_or(anyhow!("No named field in compiler"))?;
        self.vm.current_record.insert(index, value);
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
                Instruction::PushConst(idx) => {
                    self.vm.stack.push(self.vm.constants[*idx].clone());
                }
                Instruction::LoadField(idx) => {
                    self.vm.stack.push(self.vm.current_record[*idx].clone());
                }
                Instruction::Add => {
                    let r = self.vm.stack.pop().unwrap();
                    let l = self.vm.stack.pop().unwrap();
                    self.vm.stack.push(&l + &r);
                }
                Instruction::JumpIfFalse { target } => {
                    if !self.vm.stack.pop().unwrap().as_bool().unwrap().0 {
                        self.vm.pc = *target;
                        continue; // Skip the standard pc += 1
                    }
                }
                Instruction::Yield(amount) => {
                    let mut row = Vec::with_capacity(*amount);
                    if self.vm.stack.is_empty() {
                        assert_eq!(&self.vm.current_record.len(), amount);
                        for value in &self.vm.current_record {
                            row.push(value.clone());
                        }
                    } else {
                        for _ in 0..*amount {
                            row.push(self.vm.stack.pop().expect("Stack underflow at yield"));
                        }
                    }

                    row.reverse();

                    self.vm.pc += 1; // Move past Yield for the next call
                    return Some(Value::array(row));
                }
                Instruction::Equal => {
                    let r = self.vm.stack.pop().unwrap();
                    let l = self.vm.stack.pop().unwrap();
                    self.vm.stack.push(Value::bool(l == r));
                }
                Instruction::NextTuple { resource_id } => {
                    if let Some(resource) = self.vm.resources.get_mut(*resource_id)
                        && let Some(value) = resource.next()
                    {
                        self.vm.current_record = vec![value]
                    } else {
                        // we end the iterator
                        return None;
                    }
                }
                Instruction::Jump { target } => {
                    self.vm.pc = *target;
                    continue; // Skip the standard pc += 1
                }
                Instruction::Greater => {
                    let r = self.vm.stack.pop().unwrap();
                    let l = self.vm.stack.pop().unwrap();
                    self.vm.stack.push(Value::bool(l > r));
                }

                Instruction::Index => {
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
                Instruction::Minus => {
                    let r = self.vm.stack.pop().unwrap();
                    let l = self.vm.stack.pop().unwrap();
                    self.vm.stack.push(&l - &r);
                }
                Instruction::Length => {
                    let val = self.vm.stack.pop().unwrap();

                    match val {
                        Value::Array(a) => {
                            self.vm.stack.push(Value::int(a.values.len() as i64));
                        }
                        Value::Text(t) => self.vm.stack.push(Value::int(t.0.len() as i64)),
                        _ => {}
                    }
                }
                Instruction::Multiply => {
                    let r = self.vm.stack.pop().unwrap();
                    let l = self.vm.stack.pop().unwrap();
                    self.vm.stack.push(&l * &r);
                }
                Instruction::NextOrPop => {
                    if let Some(state) = self.vm.explode_stack.last_mut() {
                        state.index += 1;
                        if state.index < state.array.len() {
                            // Keep looping this array
                            self.vm.pc = state.loop_pc;
                            continue;
                        } else {
                            // This array is done
                            self.vm.explode_stack.pop();
                        }
                    }
                }
                Instruction::LoadExplodeElement => {
                    let state = self.vm.explode_stack.last().unwrap();
                    let val = state.array[state.index].clone();
                    self.vm.stack.push(val);
                }
                Instruction::InitExplode(start_pc) => {
                    let array_val = self.vm.stack.pop().unwrap();
                    if let Value::Array(arr) = array_val {
                        self.vm.explode_stack.push(ExplodeState {
                            array: arr.values,
                            index: 0,
                            loop_pc: *start_pc,
                        });
                    } else if let Value::Text(text) = array_val {
                        self.vm.explode_stack.push(ExplodeState {
                            array: text.0.chars().map(|c| Value::text(c.to_string())).collect(),
                            index: 0,
                            loop_pc: *start_pc,
                        });
                    }
                }
                Instruction::StoreField(idx) => {
                    let value = self.vm.stack.last().unwrap();
                    self.vm.current_record[*idx] = value.clone()
                }
                Instruction::Flatten => {
                    let value = self.vm.current_record.pop();
                    if let Some(value) = value {
                        match value {
                            Value::Array(a) => {
                                for val in a.values {
                                    self.vm.current_record.push(val);
                                }
                            }
                            Value::Dict(d) => match &self.compiler.current_schema {
                                Schema::Dynamic => {
                                    for v in d.values {
                                        self.vm.current_record.push(v);
                                    }
                                }
                                Schema::Fixed(f) => {
                                    for (k, _) in f {
                                        self.vm.current_record.push(d.get(k).unwrap().clone())
                                    }
                                }
                            },
                            _ => panic!(),
                        }
                    }
                }
            }
            self.vm.pc += 1;
        }
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Schema;
    use crate::operator::Operator;
    use std::vec;
    use value::ValType;

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

        let mut program = Program::from(&expr);
        program.set_record("price", Value::int(100)).unwrap();

        let mut program = program.map(|v| v.as_array().unwrap().values[0].clone());

        assert_eq!(program.next().unwrap(), Value::int(110));
    }

    #[test]
    fn test_vm_execution_filter() {
        // Simulate: price + 10
        let mut program = Program::from(&Algebra::filter(
            Algebra::scan("test", Schema::fixed([("name".to_string(), ValType::Text)])),
            Expression::Call {
                operator: Operator::Equal,
                expressions: vec![
                    Expression::Field("name".to_string()),
                    Expression::Literal(Value::int(10)),
                ],
            },
        ));

        program
            .set_resource(
                "test",
                [
                    Value::array([Value::int(10)]),
                    Value::array([Value::int(100)]),
                ]
                .into_iter(),
            )
            .unwrap();

        let mut program = program.map(|v| v.as_array().unwrap().values[0].clone());

        assert_eq!(program.next().unwrap(), Value::int(10));
        assert!(program.next().is_none());
    }

    #[test]
    fn test_vm_execution_multiple() {
        // Simulate: price + 10
        let mut program = Program::from(&Algebra::project(
            Algebra::scan("test", Schema::fixed([("name".to_string(), ValType::Text)])),
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
                ("name1".to_string(), Expression::Field("name".to_string())),
            ],
        ));

        program
            .set_resource("test", [Value::array([Value::int(100)])].into_iter())
            .unwrap();

        assert_eq!(
            program.next().unwrap(),
            Value::array([Value::int(110), Value::int(100)])
        );
    }

    #[test]
    fn test_vm_execution_sql() {
        // Simulate: price + 10
        let values = vec![
            Value::array([Value::int(3), Value::text("David"), Value::float(3.3)]),
            Value::array([Value::int(3), Value::text("David"), Value::float(5.2)]),
        ];

        let mut program = Program::from(&Algebra::project(
            Algebra::scan(
                "$$source",
                Schema::fixed([
                    ("id".to_string(), ValType::Integer),
                    ("name".to_string(), ValType::Text),
                    ("price".to_string(), ValType::Float),
                ]),
            ),
            [(
                "price".to_string(),
                Expression::Call {
                    operator: Operator::Add,
                    expressions: vec![
                        Expression::Field("price".to_string()),
                        Expression::Literal(Value::float(3.3)),
                    ],
                },
            )],
        ));

        program
            .set_resource("$$source", values.into_iter())
            .unwrap();

        let mut program = program.map(|v| v.as_array().unwrap().values[0].clone());

        assert_eq!(program.next().unwrap(), Value::float(6.6));
        assert_eq!(program.next().unwrap(), Value::float(8.5));
    }

    #[test]
    fn test_vm_execution_explode() {
        // Simulate: explode
        let mut program = Program::from(&Algebra::unwind(
            Algebra::scan("test", Schema::fixed([("name".to_string(), ValType::Text)])),
            "name",
            Operator::Explode,
        ));

        program
            .set_resource("test", [Value::array([Value::text("David")])].into_iter())
            .unwrap();

        let mut program = program.map(|v| v.as_array().unwrap().values[0].clone());

        assert_eq!(program.next().unwrap(), Value::text("D"));
        assert_eq!(program.next().unwrap(), Value::text("a"));
        assert_eq!(program.next().unwrap(), Value::text("v"));
        assert_eq!(program.next().unwrap(), Value::text("i"));
        assert_eq!(program.next().unwrap(), Value::text("d"));
    }

    #[test]
    fn test_vm_execution_explode_nested() {
        // Simulate: explode
        let mut program = Program::from(&Algebra::project(
            Algebra::unwind(
                Algebra::scan("test", Schema::fixed([("name".to_string(), ValType::Text)])),
                "name",
                Operator::Explode,
            ),
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
            .set_resource("test", [Value::array([Value::text("David")])].into_iter())
            .unwrap();

        let mut program = program.map(|v| v.as_array().unwrap().values[0].clone());

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

        let mut program = Program::from(&expr);
        program.set_record("array", Value::text("text")).unwrap();

        let mut program = program.map(|v| v.as_array().unwrap().values[0].clone());

        assert_eq!(program.next().unwrap(), Value::text("te"));
    }
}
