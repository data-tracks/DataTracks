use crate::expression::Expression;
use crate::operator::Op;
use std::collections::HashMap;
use value::Value;

pub struct VM {
    pub stack: Vec<Value>,
    pub current_record: Vec<Value>,
    pub constants: Vec<Value>, // The "Pool" for literals
}

pub struct Instruction {
    pub op: Op,
    pub arg: usize,
}

pub struct Program {
    instructions: Vec<Instruction>,
    expression: Expression,
    vm: VM
}

impl Program {

    pub fn new(expression: Expression) -> Program {
        let mut compiler = Compiler::new();
        let mut instructions = vec![];
        compiler.compile_expr(&expression.clone(), &mut instructions);

        let vm = VM {
            stack: Vec::with_capacity(16),
            current_record: vec![],
            constants: compiler.constants,
        };

        Self { instructions, expression, vm }
    }

    fn run(&mut self) -> impl Iterator<Item=Value> {
        self.vm.stack.clear();
        for instr in &self.instructions {
            (instr.op)(&mut self.vm, instr.arg);
        }
        self.vm.stack.drain(..)
    }
}



pub struct Compiler {
    pub field_map: HashMap<String, usize>,
    pub constants: Vec<Value>,
    pub next_slot: usize,
}


impl Compiler {
    pub fn new() -> Self {
        Self {
            field_map: HashMap::new(),
            constants: Vec::new(),
            next_slot: 0,
        }
    }

    pub fn compile_expr(&mut self, expr: &Expression, out: &mut Vec<Instruction>) {
        match expr {
            Expression::L(lit) => {
                let idx = self.constants.len();
                self.constants.push(lit.value.clone());
                out.push(Instruction {
                    op: op_push_const,
                    arg: idx,
                });
            }
            Expression::F(field) => {
                let instr = self.compile_field(&field.name);
                out.push(instr);
            }
            Expression::C(call) => {
                for e in &call.expressions {
                    self.compile_expr(e, out);
                }
                out.push(Instruction { op: call.operator.compile(), arg: 0 });
            }
        }
    }

    fn compile_field(&mut self, name: &str) -> Instruction {
        let slot = *self.field_map.entry(name.to_string()).or_insert_with(|| {
            let id = self.next_slot;
            self.next_slot += 1;
            id
        });

        Instruction {
            op: op_load_field,
            arg: slot,
        }
    }
}

fn op_push_const(vm: &mut VM, idx: usize) {
    let val = vm.constants[idx].clone();
    vm.stack.push(val);
}

fn op_load_field(vm: &mut VM, idx: usize) {
    let val = vm.current_record[idx].clone();
    vm.stack.push(val);
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::expression::{Call, Field, Literal};
    use crate::operator::{Binary, Operator};

    #[test]
    fn test_vm_execution_add() {
        // Simulate: price + 10
        let expr = Expression::C(Call {
            operator: Operator::binary(Binary::Add),
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

        let mut program = Program::new(expr);
        program.vm.current_record.push(Value::int(100));

        assert_eq!(program.run().next().unwrap(), Value::int(110));
    }

    #[test]
    fn test_vm_execution_array() {
        // Simulate: array[0] + array[1]
        let expr = Expression::C(Call {
            operator: Operator::binary(Binary::Add),
            expressions: vec![
                Expression::C(Call {
                    operator: Operator::binary(Binary::Index),
                    expressions: vec![
                        Expression::F(Field {
                            name: "array".into(),
                            f_type: None,
                        }),
                        Expression::L(Literal{
                            value: Value::int(0),
                        })
                    ]
                }),
                Expression::C(Call {
                    operator: Operator::binary(Binary::Index),
                    expressions: vec![
                        Expression::F(Field {
                            name: "array".into(),
                            f_type: None,
                        }),
                        Expression::L(Literal{
                            value: Value::int(1),
                        })
                    ]
                }),
            ],
        });

        let mut program = Program::new(expr);
        program.vm.current_record.push(Value::text("text"));

        assert_eq!(program.run().next().unwrap(), Value::text("te"));
    }
}
