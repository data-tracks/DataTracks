use crate::program::VM;
use crate::operator::Operator::{B, S};
use value::Value;

pub enum Step {
    Next, // IP + 1
    Stay // We stay
}

pub type Op = fn(&mut VM, arg: usize);

#[derive(Clone)]
pub enum Operator {
    S(Single),
    B(Binary),
}

impl Operator {
    pub(crate) fn compile(&self) -> Op {
        match self {
            S(s) => s.compile(),
            B(b) => b.compile()
        }
    }

    pub fn single(s: Single) -> Self {
        S(s)
    }

    pub fn binary(binary: Binary) -> Self {
        B(binary)
    }
}

fn op_add(vm: &mut VM, _: usize) {
    let b = vm.stack.pop().expect("Stack underflow");
    let a = vm.stack.pop().expect("Stack underflow");

    vm.stack.push(&a + &b);
}

fn op_sub(vm: &mut VM, _: usize) {
    let b = vm.stack.pop().expect("Stack underflow");
    let a = vm.stack.pop().expect("Stack underflow");

    vm.stack.push(&a - &b);
}

fn op_index(vm: &mut VM, _: usize) {
    let index = vm.stack.pop().expect("Stack underflow").as_int().unwrap().0 as usize;
    let array = vm.stack.pop().expect("Stack underflow");
    if let Value::Array(a) = array {
        vm.stack.push(a.values[index].clone());
    }else if let Value::Text(t) = array{
        vm.stack.push(Value::text(&t.0[index..index + 1]))
    }
}

#[derive(Clone)]
pub enum Binary {
    Add,
    Sub,
    Index,
}

impl Binary {
    pub(crate) fn compile(&self) -> Op {
        match self {
            Binary::Add => op_add,
            Binary::Sub => op_sub,
            Binary::Index => op_index
        }
    }
}

#[derive(Clone)]
pub enum Single {
    Length,
}

impl Single {
    pub(crate) fn compile(&self) -> Op {
        match self {
            Single::Length => op_len
        }
    }
}

fn op_len(vm: &mut VM, _: usize) {
    let val = vm.stack.pop().expect("Stack underflow");

    match val {
        Value::Text(t) => vm.stack.push(Value::int(t.0.len() as i64)),
        Value::Array(a) => vm.stack.push(Value::int(a.values.len() as i64)),
        _ => {}
    }
}