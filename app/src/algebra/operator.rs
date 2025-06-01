use crate::algebra::aggregate::{AvgOperator, CountOperator, SumOperator};
use crate::algebra::algebra::{BoxedValueLoader, ValueHandler};
use crate::algebra::function::{ArgImplementable, Implementable, Operator};
use crate::algebra::operator::AggOp::{Avg, Count, Sum};
use crate::algebra::operator::CollectionOp::Unwind;
use crate::algebra::operator::TupleOp::{Division, Equal, Minus, Multiplication, Not, Plus};
use crate::algebra::Op::{Agg, Binary, Collection, Tuple};
use crate::algebra::TupleOp::{And, Combine, Index, Input, Or};
use crate::algebra::{BoxedIterator, BoxedValueHandler, ValueIterator};
use crate::processing::transform::Transform;
use crate::processing::{ArrayType, DictType, Layout, OutputType, Train, TupleType};
use value::{Value};
use value::Value::{Array, Bool, Date, Dict, Float, Int, Null, Text, Time, Wagon};
use regex::Regex;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::str::FromStr;
use std::vec;
use tracing::warn;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Op {
    Binary(BinaryOp),
    Agg(AggOp),
    Tuple(TupleOp),
    Collection(CollectionOp),
}

impl Op {
    pub(crate) fn dump(&self, as_call: bool) -> String {
        match self {
            Op::Binary(b) => b.dump(as_call),
            Agg(a) => a.dump(as_call),
            Tuple(t) => t.dump(as_call),
            Collection(e) => e.dump(as_call),
        }
    }

    pub(crate) fn derive_input_layout(&self, operands: Vec<Layout>) -> Layout {
        match self {
            Agg(a) => a.derive_input_layout(operands),
            Tuple(t) => t.derive_input_layout(operands),
            Collection(e) => e.derive_input_layout(operands),
            Op::Binary(b) => b.derive_input_layout(operands),
        }
    }

    pub(crate) fn derive_output_layout(
        &self,
        operands: Vec<Layout>,
        inputs: HashMap<String, &Layout>,
    ) -> Layout {
        match self {
            Agg(a) => a.derive_output_layout(operands, inputs),
            Tuple(t) => t.derive_output_layout(operands, inputs),
            Collection(e) => e.derive_output_layout(operands, inputs),
            Op::Binary(b) => b.derive_output_layout(operands, inputs),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CollectionOp {
    Unwind,
}

impl CollectionOp {
    pub fn implement(&self, input: BoxedIterator, operators: Vec<Operator>) -> BoxedIterator {
        match self {
            Unwind => {
                let op = match operators.len() {
                    1 => operators.first().unwrap(),
                    _ => &Operator::combine(operators),
                };
                Box::new(SetProjectIterator::new(input, op.implement().unwrap()))
            }
        }
    }

    pub(crate) fn dump(&self, _as_call: bool) -> String {
        match self {
            Unwind => "UNWIND".to_string(),
        }
    }

    pub(crate) fn derive_output_layout(
        &self,
        operands: Vec<Layout>,
        _inputs: HashMap<String, &Layout>,
    ) -> Layout {
        match self {
            Unwind => operands.first().cloned().unwrap_or_default(),
        }
    }

    pub(crate) fn derive_input_layout(&self, operands: Vec<Layout>) -> Layout {
        match self {
            Unwind => {
                if operands.is_empty() {
                    Layout::array(None)
                } else {
                    operands.first().unwrap().clone()
                }
            }
        }
    }
}

pub struct SetProjectIterator {
    input: BoxedIterator,
    values: Vec<Value>,
    before_project: BoxedValueHandler,
}

impl SetProjectIterator {
    pub fn new(input: BoxedIterator, before_project: BoxedValueHandler) -> Self {
        Self {
            input,
            values: vec![],
            before_project,
        }
    }
}

impl Iterator for SetProjectIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.values.is_empty() {
                return Some(self.values.remove(0));
            }
            match self.input.next() {
                None => return None,
                Some(values) => {
                    self.values.append(&mut unwind(values));
                }
            }
        }
    }
}

fn unwind<'a>(value: Value) -> Vec<Value> {
    match value {
        Array(a) => a.values.clone(),
        Dict(d) => d.iter().map(|(_, v)| v.clone()).collect(),
        Wagon(w) => unwind(w.unwrap()),
        v => vec![v],
    }
}

impl ValueIterator for SetProjectIterator {
    fn dynamically_load(&mut self, train: Train) {
        self.input.dynamically_load(train);
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(SetProjectIterator::new(
            self.input.clone(),
            self.before_project.clone(),
        ))
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TupleOp {
    Plus,
    Minus,
    Multiplication,
    Division,
    Combine,
    Not,
    Equal,
    And,
    Or,
    Doc,
    Split(SplitOp),
    Input(InputOp),
    Name(NameOp),
    Index(IndexOp),
    Literal(LiteralOp),
    Context(ContextOp),
    KeyValue(Option<String>),
}


impl TupleOp {
    pub fn implement(&self, operators: Vec<Operator>) -> BoxedValueHandler {
        let operands = operators
            .into_iter()
            .map(|o| o.implement().unwrap())
            .collect();
        match self {
            Plus => Box::new(TupleFunction::new(
                |value| value.iter().fold(Value::int(0), |a, b| &a + b),
                operands,
            )),

            Minus => Box::new(TupleFunction::new(
                move |value| {
                    let a = value.first().unwrap();
                    let b = value.get(1).unwrap();
                    a - b
                },
                operands,
            )),
            Multiplication => Box::new(TupleFunction::new(
                move |value| value.iter().fold(Value::int(1), |a, b| &a * b),
                operands,
            )),
            Division => Box::new(TupleFunction::new(
                move |value| {
                    let a = value.first().unwrap();
                    let b = value.get(1).unwrap();
                    a / b
                },
                operands,
            )),
            Equal => Box::new(TupleFunction::new(
                move |value| {
                    let a = value.first().unwrap();
                    let b = value.get(1).unwrap();
                    (a.clone() == b.clone()).into()
                },
                operands,
            )),
            Combine | TupleOp::KeyValue(_) => Box::new(TupleFunction::new(
                move |value| Value::array(value.iter().map(|v| (*v).clone()).collect()),
                operands,
            )),
            Not => Box::new(TupleFunction::new(
                move |vec| {
                    let value = Value::bool(vec.first().unwrap().as_bool().unwrap().0);
                    match vec.first().unwrap() {
                        Int(_) => Int(value.as_int().unwrap()),
                        Float(_) => Float(value.as_float().unwrap()),
                        Bool(_) => Bool(value.as_bool().unwrap()),
                        Text(_) => Text(value.as_text().unwrap()),
                        Array(_) => Array(value.as_array().unwrap()),
                        Dict(_) => Dict(value.as_dict().unwrap()),
                        Null => Value::null(),
                        Wagon(_) => panic!(),
                        Time(_) => Time(value.as_time().unwrap()),
                        Date(d) => Value::date(-d.days),
                    }
                },
                operands,
            )),
            And => Box::new(TupleFunction::new(
                move |value| {
                    value.iter().fold(Value::bool(true), |a, b| {
                        (a.as_bool().unwrap().0 && b.as_bool().unwrap().0).into()
                    })
                },
                operands,
            )),
            Or => Box::new(TupleFunction::new(
                move |value| {
                    value.iter().fold(Value::bool(true), |a, b| {
                        (a.as_bool().unwrap().0 || b.as_bool().unwrap().0).into()
                    })
                },
                operands,
            )),
            Input(i) => ValueHandler::clone(i),
            TupleOp::Name(n) => n.implement().unwrap(),
            Index(i) => i.implement().unwrap(),
            TupleOp::Literal(lit) => lit.implement().unwrap(),
            TupleOp::Context(c) => c.implement().unwrap(),
            TupleOp::Split(_) => Box::new(TupleFunction::new(
                move |value| {
                    let text = value.first().unwrap();
                    let regex = value.get(1).unwrap().as_text().unwrap().0;
                    SplitOp::split(text, &regex)
                },
                operands,
            )),
            TupleOp::Doc => Box::new(TupleFunction::new(
                move |value| {
                    let mut map = BTreeMap::new();
                    value.iter().for_each(|k| {
                        let pair = k.as_array().unwrap();
                        map.insert(pair.values[0].as_text().unwrap().0, pair.values[1].clone());
                    });
                    Value::dict(map)
                },
                operands,
            )),
        }
    }

    pub(crate) fn derive_input_layout(&self, operands: Vec<Layout>) -> Layout {
        match self {
            Plus | Minus | Multiplication | Division | Equal => {
                let left = operands.first().cloned().unwrap_or(Layout::default());
                let _right = operands.get(1).cloned().unwrap_or(Layout::default());
                left
            }
            Not | And | Or => Layout::from(OutputType::Boolean),
            Combine => operands
                .iter()
                .fold(Layout::default(), |a, b| a.clone().merge(b)),
            TupleOp::KeyValue(_) => {
                let first = operands.first().cloned().unwrap_or(Layout::default());
                let second = operands.get(1).cloned().unwrap_or(Layout::default());
                first.merge(&second)
            }
            TupleOp::Doc => operands
                .iter()
                .fold(Layout::default(), |a, b| a.clone().merge(b)),
            Input(_) => Layout::default(),
            TupleOp::Split(_) => Layout::array(None),
            TupleOp::Name(n) => {
                let mut map = vec![];
                map.push(Layout::from(n.name.as_str()));
                let dict = OutputType::Dict(Box::new(DictType::new(map)));

                Layout::from(dict)
            }
            Index(i) => {
                let array = ArrayType::new(Layout::default(), Some((i.index + 1) as i32));

                Layout::from(OutputType::Array(Box::new(array)))
            }
            TupleOp::Literal(_) => Layout::default(),
            TupleOp::Context(_) => Layout::default(),
        }
    }

    pub(crate) fn derive_output_layout(
        &self,
        operands: Vec<Layout>,
        inputs: HashMap<String, &Layout>,
    ) -> Layout {
        match self {
            Plus | Minus | Multiplication | Division => {
                let left = operands.first().cloned().unwrap_or_default();
                let _right = operands.get(1).cloned().unwrap_or_default();
                left
            }
            Combine => {
                let mut layout = Layout::default();
                layout.type_ = OutputType::Tuple(Box::new(TupleType::from(operands)));
                layout
            }
            TupleOp::KeyValue(n) => {
                let _key = operands.first().cloned().unwrap_or_default();
                let mut value = operands.get(1).cloned().unwrap_or_default();
                value.name = n.clone();
                let fields = vec![value];
                Layout::from(OutputType::Dict(Box::new(DictType::new(fields))))
            }
            Not | Equal | And | Or => Layout::from(OutputType::Boolean),
            TupleOp::Doc => operands.into_iter().fold(
                Layout::from(OutputType::Dict(Box::new(DictType::new(vec![])))),
                |a, b| a.merge(&b),
            ),
            Input(_) => Layout::default(),
            TupleOp::Split(_) => {
                let layout = operands.first().cloned().unwrap_or_default();
                match layout.type_ {
                    OutputType::Text => layout,
                    _ => layout,
                }
            }
            TupleOp::Name(n) => {
                let layout = operands.first().cloned().unwrap_or_default();
                match layout.type_.clone() {
                    OutputType::Dict(d) => d.get(&n.name).cloned().unwrap_or_default(),
                    _ => Layout::from(n.name.clone().as_str()),
                }
            }
            Index(i) => {
                let layout = operands.first().unwrap();
                match layout.type_.clone() {
                    OutputType::Array(a) => a.fields,
                    OutputType::Dict(d) => {
                        let names = d.names();
                        let mut keys = names.iter();
                        for _ in 0..i.index {
                            keys.next().unwrap();
                        }
                        d.get(keys.next().unwrap()).unwrap().clone()
                    }
                    _ => Layout::default(),
                }
            }
            TupleOp::Literal(l) => Layout {
                type_: OutputType::from(&l.literal.clone()),
                ..Default::default()
            },
            TupleOp::Context(c) => (*inputs.get(&c.name).unwrap()).clone(),
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
            Division => {
                if as_call {
                    String::from("DIVIDE")
                } else {
                    String::from("/")
                }
            }
            Combine | TupleOp::KeyValue(_) => String::from(""),
            Not => String::from("NOT"),
            Equal => {
                if as_call {
                    String::from("EQ")
                } else {
                    String::from("=")
                }
            }
            And => String::from("AND"),
            Or => String::from("OR"),
            Input(_) => String::from("*"),
            TupleOp::Name(name) => name.name.clone(),
            Index(i) => i.index.to_string(),
            TupleOp::Literal(value) => value.literal.to_string(),
            TupleOp::Context(c) => {
                format!("${}", c.name)
            }
            TupleOp::Doc => "".to_string(),
            TupleOp::Split(_) => {
                "SPLIT".to_string()
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Cast
}


impl BinaryOp {
    pub(crate) fn dump(&self, _as_call: bool) -> String {
        match self {
            BinaryOp::Cast => "CAST".to_string(),
        }
    }

    pub(crate) fn derive_input_layout(&self, _operands: Vec<Layout>) -> Layout {
        match self {
            BinaryOp::Cast => Layout::from(OutputType::Or(vec![
                OutputType::Integer,
                OutputType::Float,
                OutputType::Boolean,
                OutputType::Text,
            ])),
        }
    }

    pub(crate) fn derive_output_layout(
        &self,
        operands: Vec<Layout>,
        _inputs: HashMap<String, &Layout>,
    ) -> Layout {
        match self {
            BinaryOp::Cast => {
                match operands.get(1) {
                    None => Layout::default(),
                    Some(o) => o.clone(),
                }
            }
        }
    }

}

impl ValueHandler for BinaryOp {
    fn process(&self, _value: &Value) -> Value {
        todo!()
    }

    fn clone(&self) -> BoxedValueHandler {
        todo!()
    }
}

impl ArgImplementable<BoxedValueHandler, Vec<Operator>> for BinaryOp {
    fn implement(&self, operators: Vec<Operator>) -> Result<BoxedValueHandler, ()> {
        let operands = operators
            .into_iter()
            .map(|o| o.implement().unwrap())
            .collect();
        match self {
            BinaryOp::Cast => Ok(Box::new(TupleFunction::new(
                |value| value.iter().fold(Value::int(0), |a, b| &a + b),
                operands,
            ))),
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggOp {
    Count,
    Sum,
    Avg,
}

impl AggOp {
    pub(crate) fn dump(&self, _as_call: bool) -> String {
        match self {
            Count => "COUNT".to_string(),
            Sum => "SUM".to_string(),
            Avg => "AVG".to_string(),
        }
    }

    pub(crate) fn derive_input_layout(&self, _operands: Vec<Layout>) -> Layout {
        match self {
            Count => Layout::default(),
            Sum => Layout::from(OutputType::Or(vec![
                OutputType::Integer,
                OutputType::Float,
                OutputType::Boolean,
                OutputType::Text,
            ])),
            Avg => Layout::from(OutputType::Or(vec![
                OutputType::Integer,
                OutputType::Float,
                OutputType::Boolean,
                OutputType::Text,
            ])),
        }
    }

    pub(crate) fn derive_output_layout(
        &self,
        _operands: Vec<Layout>,
        _inputs: HashMap<String, &Layout>,
    ) -> Layout {
        match self {
            Count => Layout::from(OutputType::Integer),
            Sum => Layout::from(OutputType::Float),
            Avg => Layout::from(OutputType::Float),
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
        Box::new(TupleFunction::new(
            self.func,
            self.children.iter().map(|c| (*c).clone()).collect(),
        ))
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
            "/" | "divide" => Ok(Tuple(Division)),
            "count" => Ok(Agg(Count)),
            "sum" => Ok(Agg(Sum)),
            "avg" => Ok(Agg(Avg)),
            "unwind" => Ok(Collection(Unwind)),
            "split" => Ok(Tuple(TupleOp::Split(SplitOp::new()))),
            "cast" => Ok(Binary(BinaryOp::Cast)),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexOp {
    pub index: usize,
}

impl IndexOp {
    pub fn new(index: usize) -> Self {
        IndexOp { index }
    }
}

impl ValueHandler for IndexOp {
    fn process(&self, value: &Value) -> Value {
        match value {
            Array(a) => a.values.get(self.index).unwrap_or(&Value::null()).clone(),
            Dict(d) => d
                .get(&format!("${}", self.index))
                .unwrap_or(&Value::null())
                .clone(),
            Null => Value::null(),
            Wagon(w) => self.process(&w.value),
            _ => panic!("Could not process {}", value),
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        Box::new(LiteralOp {
            literal: self.literal.clone(),
        })
    }
}

impl Implementable<BoxedValueHandler> for LiteralOp {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(ValueHandler::clone(self))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ContextOp {
    pub name: String,
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
            }
            Array(a) => {
                let mut array =
                    a.values.iter()
                        .filter(|v| match v {
                            Wagon(w) => w.origin == self.name,
                            _ => false,
                        })
                        .cloned()
                        .map(|w| match w {
                            Wagon(w) => w.unwrap(),
                            _ => panic!(),
                        })
                        .collect::<Vec<_>>();
                if array.len() == 1 {
                    array.pop().unwrap()
                } else {
                    Value::array(array)
                }
            }
            Dict(d) => {
                let map = BTreeMap::from_iter(
                    d.iter()
                        .filter(|(_k, v)| match v {
                            Wagon(w) => w.origin == self.name,
                            _ => false,
                        })
                        .map(|(k, v)| match v {
                            Wagon(w) => (k.clone(), w.clone().unwrap()),
                            _ => panic!(),
                        }),
                );
                Value::dict(map)
            }
            _ => panic!("Could not process {}", value),
        }
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(ContextOp {
            name: self.name.clone(),
        })
    }
}

impl Implementable<BoxedValueHandler> for ContextOp {
    fn implement(&self) -> Result<BoxedValueHandler, ()> {
        Ok(ValueHandler::clone(self))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InputOp {}

impl ValueHandler for InputOp {
    fn process(&self, value: &Value) -> Value {
        value.clone()
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(InputOp {})
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SplitOp {}

impl SplitOp {
    pub fn new() -> Self {
        SplitOp {}
    }

    fn split(value: &Value, regex: &str) -> Value {
        let re = Regex::new(regex).unwrap();
        match value {
            Text(t) => Value::array(re.split(&t.0).collect::<Vec<_>>().into_iter().map(|v| v.into()).collect::<Vec<_>>(),
            ),
            Wagon(w) => Self::split(&w.clone().unwrap(), regex),
            v => Self::split(&Text(v.as_text().unwrap()), regex)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
            v => {
                warn!("Could not process {} with key {}", v, self.name);
                Null
            },
        }
    }

    fn clone(&self) -> BoxedValueHandler {
        Box::new(NameOp {
            name: self.name.clone(),
        })
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
            Array(a) => a.values.get(self.index).cloned().unwrap(),
            Null => Value::null(),
            _ => panic!(),
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
        Tuple(Division)
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

#[cfg(test)]
mod tests {
    use crate::algebra::operator::{IndexOp, LiteralOp, NameOp};
    use crate::algebra::Op::Tuple;
    use crate::algebra::Operator;
    use crate::algebra::TupleOp::{
        And, Division, Equal, Index, Literal, Minus, Multiplication, Name, Not, Or, Plus,
    };
    use crate::analyse::{InputDerivable, OutputDerivable};
    use crate::processing::OutputType::{Array, Dict};
    use crate::processing::{ArrayType, DictType, Layout, OutputType};
    use value::Value;
    use std::collections::HashMap;
    use std::vec;

    #[test]
    fn test_layout_literal() {
        let op = Literal(LiteralOp::new(Value::text("test")));

        assert_eq!(
            op.derive_output_layout(vec![], HashMap::new()),
            Layout::from(OutputType::Text)
        );
        assert_eq!(op.derive_input_layout(vec![]), Layout::default());

        let op = Literal(LiteralOp::new(Value::dict_from_pairs(vec![(
            "test",
            Value::text("test"),
        )])));
        let mut dict = Vec::new();
        dict.push(Layout::from(("test", OutputType::Text)));
        assert_eq!(
            op.derive_output_layout(vec![], HashMap::new()),
            Layout::from(Dict(Box::new(DictType::new(dict))))
        );
    }

    #[test]
    fn test_layout_index() {
        let op = Index(IndexOp::new(3));
        let mut layout = Layout::default();
        layout.type_ = Array(Box::new(ArrayType::new(Layout::default(), Some(4))));
        assert_eq!(op.derive_input_layout(vec![]), layout);
        let array = Layout::from(Array(Box::new(ArrayType::new(
            Layout::from(OutputType::Integer),
            Some(4),
        ))));
        assert_eq!(
            op.derive_output_layout(vec![array], HashMap::new()),
            Layout::from(OutputType::Integer)
        );
    }

    #[test]
    fn test_layout_name() {
        let op = Name(NameOp::new(String::from("test")));

        let mut map = Vec::new();
        map.push(Layout::from("test"));
        let mut layout = Layout::default();
        layout.type_ = Dict(Box::new(DictType::new(map)));
        assert_eq!(op.derive_input_layout(vec![]), layout);

        let mut map = Vec::new();
        map.push(Layout::from(("test", OutputType::Float)));
        let dict = Layout::from(Dict(Box::new(DictType::new(map))));
        assert_eq!(
            op.derive_output_layout(vec![dict], HashMap::new()),
            Layout::from(("test", OutputType::Float))
        );
    }

    #[test]
    fn test_layout_binary_op() {
        let ops = vec![
            Tuple(Multiplication),
            Tuple(Division),
            Tuple(Minus),
            Tuple(Plus),
        ];
        for op in ops {
            assert_eq!(op.derive_input_layout(vec![]), Layout::default());
            assert_eq!(
                op.derive_output_layout(
                    vec![
                        Layout::from(OutputType::Integer),
                        Layout::from(OutputType::Integer)
                    ],
                    HashMap::default()
                ),
                Layout::from(OutputType::Integer)
            );
        }
        let op = Tuple(Plus);
        assert_eq!(
            op.derive_output_layout(
                vec![
                    Layout::from(OutputType::Text),
                    Layout::from(OutputType::Integer)
                ],
                HashMap::default()
            ),
            Layout::from(OutputType::Text)
        );
    }

    #[test]
    fn test_layout_boolean_op() {
        let ops = vec![Tuple(Or), Tuple(And), Tuple(Not), Tuple(Equal)];
        for op in ops {
            assert_eq!(
                op.derive_input_layout(vec![Layout::from(OutputType::Boolean)]),
                Layout::from(OutputType::Boolean)
            );
            assert_eq!(
                op.derive_output_layout(vec![], HashMap::default()),
                Layout::from(OutputType::Boolean)
            );
        }
    }

    #[test]
    fn test_layout_tuple() {
        let op = Operator::combine(vec![
            Operator::name("key1", vec![Operator::input()]),
            Operator::name("key2", vec![Operator::input()]),
        ]);

        let mut layout = Layout::default();
        let mut map = Vec::new();
        map.push(Layout::from("key1"));
        map.push(Layout::from("key2"));
        layout.type_ = Dict(Box::new(DictType::new(map)));

        assert_eq!(op.derive_input_layout().unwrap(), layout);

        let array = Layout::tuple(vec![Some("key1".to_string()), Some("key2".to_string())]);
        assert_eq!(op.derive_output_layout(HashMap::new()).unwrap(), array);
    }
}
