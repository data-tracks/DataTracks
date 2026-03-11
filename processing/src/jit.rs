use crate::expression::Expression;
use std::collections::HashMap;
use value::Value;

pub type Op = fn(&mut VM, arg: usize);

pub struct VM {
    pub stack: Vec<Value>,
    pub current_record: Vec<Value>,
    pub constants: Vec<Value>, // The "Pool" for literals
}

pub struct Instruction {
    pub op: Op,
    pub arg: usize,
}

pub struct Compiler {
    pub field_map: HashMap<String, usize>,
    pub constants: Vec<Value>,
    pub next_slot: usize,
}

// --- Instructions (The "Hot Path" functions) ---

fn op_load_field(vm: &mut VM, slot_idx: usize) {
    let val = vm.current_record[slot_idx].clone();
    vm.stack.push(val);
}

fn op_push_const(vm: &mut VM, idx: usize) {
    let val = vm.constants[idx].clone();
    vm.stack.push(val);
}

fn op_add(vm: &mut VM, _: usize) {
    let b = vm.stack.pop().expect("Stack underflow");
    let a = vm.stack.pop().expect("Stack underflow");

    match (a, b) {
        (Value::Int(v1), Value::Int(v2)) => vm.stack.push(Value::Int(v1 + v2)),
        (Value::Text(t1), Value::Text(t2)) => vm.stack.push(Value::text(&format!("{}{}", t1, t2))),
        _ => panic!("Type mismatch in add"),
    }
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
                // For simplicity, mapping all binary calls to op_add
                out.push(Instruction { op: op_add, arg: 0 });
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::expression::{Call, Field, Literal};
    use crate::operator::{Binary, Operator};

    #[test]
    fn test_vm_execution() {
        let mut compiler = Compiler::new();

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

        let mut program = Vec::new();
        compiler.compile_expr(&expr, &mut program);

        let mut vm = VM {
            stack: Vec::with_capacity(16),
            current_record: vec![Value::int(100)], // "price" is at index 0
            constants: compiler.constants,         // Transfer literals to VM
        };

        // Execution
        for instr in &program {
            (instr.op)(&mut vm, instr.arg);
        }

        let result = vm.stack.pop().unwrap();
        if let Value::Int(v) = result {
            assert_eq!(v.0, 110);
        } else {
            panic!("Expected Int");
        }
    }
}
