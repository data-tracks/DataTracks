use crate::algebra::aggregate::{AvgOperator, CountOperator, SumOperator};
use crate::algebra::algebra::{BoxedValueLoader, ValueHandler};
use crate::algebra::function::{Implementable, Operator};
use crate::algebra::operator::AggOp::{Avg, Count, Sum};
use crate::algebra::operator::TupleOp::{Divide, Equal, Minus, Multiplication, Not, Plus};
use crate::algebra::Op::{Agg, Tuple};
use crate::algebra::TupleOp::{And, Combine, Index, Input, Or};
use crate::algebra::{BoxedIterator, BoxedValueHandler};
use crate::processing::transform::Transform;
use crate::value::Value;
use crate::value::Value::{Array, Bool, Dict, Float, Int, Null, Text, Wagon};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Agg(AggOp),
    Tuple(TupleOp),
}


impl Op {
    pub(crate) fn dump(&self, as_call: bool) -> String {
        match self {
            Agg(a) => a.dump(as_call),
            Tuple(t) => t.dump(as_call),
        }
    }

    pub(crate) fn implement(&self, operators: Vec<Operator>) -> BoxedValueHandler {
        match self {
            Agg(_) => panic!("Aggregations should have been replaced!"),
            Tuple(t) => t.implement(operators),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TupleOp {
    Plus,
    Minus,
    Multiplication,
    Divide,
    Combine,
    Not,
    Equal,
    And,
    Or,
    Doc,
    Input(InputOp),
    Name(NameOp),
    Index(IndexOp),
    Literal(LiteralOp),
    Context(ContextOp),
}


impl TupleOp {
    pub fn implement(&self, operators: Vec<Operator>) -> BoxedValueHandler {
        let operands = operators.into_iter().map(|o| o.implement().unwrap()).collect();
        match self {
            Plus => {
                Box::new(
                    TupleFunction::new(|value| {
                        value.iter().fold(Value::int(0), |a, b| {
                            &a + b
                        })
                    }, operands)
                )
            }

            Minus => {
                Box::new(
                    TupleFunction::new(move |value| {
                        let a = value.get(0).unwrap();
                        let b = value.get(1).unwrap();
                        a - b
                    }, operands)
                )
            }
            Multiplication => {
                Box::new(
                    TupleFunction::new(move |value| {
                        value.iter().fold(Value::int(1), |a, b| {
                            &a * b
                        })
                    }, operands)
                )
            }
            Divide => {
                Box::new(
                    TupleFunction::new(move |value| {
                        let a = value.get(0).unwrap();
                        let b = value.get(1).unwrap();
                        a / b
                    }, operands)
                )
            }
            Equal => {
                Box::new(
                    TupleFunction::new(move |value| {
                        let a = value.get(0).unwrap();
                        let b = value.get(1).unwrap();
                        (a.clone() == b.clone()).into()
                    }, operands)
                )
            }
            Combine => {
                Box::new(
                    TupleFunction::new(move |value| {
                        Value::array(value.iter().map(|v| (*v).clone()).collect())
                    }, operands)
                )
            }
            Not => {
                Box::new(
                    TupleFunction::new(move |vec| {
                        let value = Value::bool(vec.get(0).unwrap().as_bool().unwrap().0);
                        match vec.get(0).unwrap() {
                            Int(_) => Int(value.as_int().unwrap()),
                            Float(_) => Float(value.as_float().unwrap()),
                            Bool(_) => Bool(value.as_bool().unwrap()),
                            Text(_) => Text(value.as_text().unwrap()),
                            Array(_) => Array(value.as_array().unwrap()),
                            Dict(_) => Dict(value.as_dict().unwrap()),
                            Null => Value::null(),
                            Wagon(_) => panic!()
                        }
                    }, operands)
                )
            }
            And => {
                Box::new(
                    TupleFunction::new(move |value| {
                        value.iter().fold(Value::bool(true), |a, b| {
                            (a.as_bool().unwrap().0 && b.as_bool().unwrap().0).into()
                        })
                    }, operands)
                )
            }
            Or => {
                Box::new(
                    TupleFunction::new(move |value| {
                        value.iter().fold(Value::bool(true), |a, b| {
                            (a.as_bool().unwrap().0 || b.as_bool().unwrap().0).into()
                        })
                    }, operands)
                )
            }
            Input(i) => {
                ValueHandler::clone(i)
            }
            TupleOp::Name(n) => {
                n.implement().unwrap()
            }
            Index(i) => {
                i.implement().unwrap()
            }
            TupleOp::Literal(lit) => {
                lit.implement().unwrap()
            }
            TupleOp::Context(c) => {
                c.implement().unwrap()
            }
            TupleOp::Doc => {
                Box::new(
                    TupleFunction::new(move |value| {
                        let mut map = BTreeMap::new();
                        value.iter().for_each(|k| {
                            let pair = k.as_array().unwrap();
                            map.insert(pair.0[0].as_text().unwrap().0, pair.0[1].clone());
                        });
                        Value::dict(map)
                    }, operands)
                )
            }
        }
    }


    pub fn dump(&self, as_call: bool) -> String {
        match self {
            Plus => {
                if as_call {
                    String::from("ADD")
                } else {
                    String::from("+")
                }
            }
            Minus => {
                if as_call {
                    String::from("MINUS")
                } else {
                    String::from("-")
                }
            }
            Multiplication => {
                if as_call {
                    String::from("MULTIPLICATION")
                } else {
                    String::from("*")
                }
            }
            Divide => {
                if as_call {
                    String::from("DIVIDE")
                } else {
                    String::from("/")
                }
            }
            Combine => {
                String::from("")
            }
            Not => {
                String::from("NOT")
            }
            Equal => {
                if as_call {
                    String::from("EQ")
                } else {
                    String::from("=")
                }
            }
            And => {
                String::from("AND")
            }
            Or => {
                String::from("OR")
            }
            Input(_) => {
                String::from("*")
            }
            TupleOp::Name(name) => {
                name.name.clone()
            }
            Index(i) => {
                i.index.to_string()
            }
            TupleOp::Literal(value) => {
                value.literal.to_string()
            }
            TupleOp::Context(c) => {
                format!("${}", c.name)
            }
            TupleOp::Doc => {
                "".to_string()
            }
        }
    }
}


#[derive(Debug, Clone, PartialEq)]
pub enum AggOp {
    Count,
    Sum,
    Avg
}

impl AggOp {
    pub(crate) fn dump(&self, _as_call: bool) -> String {
        match self {
            Count => "COUNT".to_string(),
            Sum => "SUM".to_string(),
            Avg => "AVG".to_string()
        }
    }
}

impl Implementable<BoxedValueLoader> for AggOp {
    fn implement(&self) -> Result<BoxedValueLoader, ()> {
        match self {
            Count => Ok(Box::new(CountOperator::new())),
            Sum => Ok(Box::new(SumOperator::new())),
            Avg => Ok(Box::new(AvgOperator::new())),
        }
    }
}


pub struct TupleFunction {
    func: fn(&Vec<Value>) -> Value,
    children: Vec<BoxedValueHandler>,
}

impl TupleFunction {
    pub fn new(func: fn(&Vec<Value>) -> Value, children: Vec<BoxedValueHandler>) -> Self {
        TupleFunction { func, children }
    }
}


impl ValueHandler for TupleFunction {
    fn process(&self, value: &Value) -> Value {
        let children = self.children.iter().map(|c| c.process(value)).collect();
        (self.func)(&children)
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(TupleFunction::new(self.func, self.children.iter().map(|c| (*c).clone()).collect()))
    }
}


impl FromStr for Op {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut trimmed = s.to_lowercase();
        if s.ends_with('(') {
            trimmed.pop();
        }
        match trimmed.as_str() {
            "+" | "add" | "plus" => Ok(Tuple(Plus)),
            "-" | "minus" => Ok(Tuple(Minus)),
            "*" | "multiply" => Ok(Tuple(Multiplication)),
            "/" | "divide" => Ok(Tuple(Divide)),
            "count" => Ok(Agg(Count)),
            "sum" => Ok(Agg(Sum)),
            "avg" => Ok(Agg(Avg)),
            _ => Err(())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexOp {
    index: usize,
}

impl IndexOp {
    pub fn new(index: usize) -> Self {
        IndexOp {
            index,
        }
    }
}

impl ValueHandler for IndexOp {
    fn process(&self, value: &Value) -> Value {
        match value {
            Array(a) => {
                a.0.get(self.index).unwrap_or(&Value::null()).clone()
            }
            Dict(d) => {
                d.get(&format!("${}", self.index)).unwrap_or(&Value::null()).clone()
            }
            Null => Value::null(),
            Wagon(w) => self.process(&w.value),
            _ => panic!("Could not process {}", value)
        }
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(IndexOp { index: self.index })
    }
}

impl Implementable<BoxedValueHandler> for IndexOp {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(ValueHandler::clone(self))
    }
}


#[derive(Debug, Clone, PartialEq)]
pub struct LiteralOp {
    pub literal: Value,
}

impl LiteralOp {
    pub fn new(literal: Value) -> LiteralOp {
        LiteralOp { literal }
    }
}


impl ValueHandler for LiteralOp {
    fn process(&self, _value: &Value) -> Value {
        self.literal.clone()
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(LiteralOp { literal: self.literal.clone() })
    }
}

impl Implementable<BoxedValueHandler> for LiteralOp {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(ValueHandler::clone(self))
    }
}

pub struct VariableOp {
    name: String,
    transform: Option<Transform>,
    operator: Option<BoxedIterator>
}

impl Clone for VariableOp {
    fn clone(&self) -> Self {
        VariableOp{name: self.name.clone(), transform: None, operator: None}
    }
}

impl Debug for VariableOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("${}", self.name).as_str())
    }
}

impl PartialEq for VariableOp {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl VariableOp {
    pub fn new(name: String) -> Self {
        VariableOp { name, transform: None, operator: None }
    }

    pub fn set_transform(&mut self, transform: Transform) {
        self.operator = Some(transform.optimize(HashMap::new()));
        self.transform = Some(transform);
    }
}


#[derive(Clone, Debug, PartialEq)]
pub struct ContextOp {
    name: String,
}

impl ContextOp {
    pub fn new(name: String) -> Self {
        ContextOp { name }
    }
}

impl ValueHandler for ContextOp {
    fn process(&self, value: &Value) -> Value {
        match value {
            Wagon(w) => {
                if w.origin == self.name {
                    *w.value.clone()
                } else {
                    panic!("Could not process {:?}", w)
                }
            },
            Array(a) => {
                let mut array = a.0.iter().filter(|v| {
                    match v {
                        Wagon(w) => w.origin == self.name,
                        _ => false
                    }
                }).cloned().map(|w| {
                    match w {
                        Wagon(w) => w.unwrap(),
                        _ => panic!()
                    }
                }).collect::<Vec<_>>();
                if array.len() == 1 {
                    array.pop().unwrap()
                } else {
                    Value::array(array)
                }
            }
            Dict(d) => {
                let map = BTreeMap::from_iter(d.iter().filter(|(_k, v)| {
                    match v {
                        Wagon(w) => w.origin == self.name,
                        _ => false
                    }
                }).map(|(k, v)| {
                    match v {
                        Wagon(w) => {
                            (k.clone(), w.clone().unwrap())
                        },
                        _ => panic!()
                    }
                }));
                Value::dict(map)
            }
            _ => panic!("Could not process {}", value)
        }
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(ContextOp { name: self.name.clone() })
    }
}

impl Implementable<BoxedValueHandler> for ContextOp {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(ValueHandler::clone(self))
    }
}


#[derive(Clone, Debug, PartialEq)]
pub struct InputOp {}

impl ValueHandler for InputOp {
    fn process(&self, value: &Value) -> Value {
        value.clone()
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(InputOp {})
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NameOp {
    pub name: String,
}

impl NameOp {
    pub fn new(name: String) -> NameOp {
        NameOp { name }
    }
}

impl ValueHandler for NameOp {
    fn process(&self, value: &Value) -> Value {
        match value {
            Dict(d) => d.get(&self.name).unwrap_or(&Value::null()).clone(),
            Null => Value::null(),
            Wagon(w) => self.process(w.value.as_ref()),
            v => panic!("Could not process {} with key {}", v, self.name)
        }
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(NameOp { name: self.name.clone() })
    }
}

impl Implementable<BoxedValueHandler> for NameOp {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(ValueHandler::clone(self))
    }
}


#[derive(Debug, Clone)]
pub struct IndexedRefOperator {
    pub index: usize,
}

impl ValueHandler for IndexedRefOperator {
    fn process(&self, value: &Value) -> Value {
        match value {
            Array(a) => a.0.get(self.index).cloned().unwrap(),
            Null => Value::null(),
            _ => panic!()
        }
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(IndexedRefOperator { index: self.index })
    }
}

impl Implementable<BoxedValueHandler> for IndexedRefOperator {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(ValueHandler::clone(self))
    }
}


impl Op {
    pub fn plus() -> Op {
        Tuple(Plus)
    }
    pub fn minus() -> Op {
        Tuple(Minus)
    }
    pub fn multiply() -> Op {
        Tuple(Multiplication)
    }
    pub fn divide() -> Op {
        Tuple(Divide)
    }

    pub fn equal() -> Op {
        Tuple(Equal)
    }

    pub fn not() -> Op {
        Tuple(Not)
    }

    pub fn and() -> Op {
        Tuple(And)
    }
    pub fn or() -> Op {
        Tuple(Or)
    }

    pub(crate) fn combine() -> Op {
        Tuple(Combine)
    }

    pub(crate) fn index(index: usize) -> Op {
        Tuple(Index(IndexOp::new(index)))
    }

    pub(crate) fn input() -> Op {
        Tuple(Input(InputOp {}))
    }
}




