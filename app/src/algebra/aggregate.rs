use crate::algebra::algebra::BoxedValueLoader;
use crate::algebra::function::Implementable;
use crate::algebra::operator::{AggOp, IndexOp};
use crate::algebra::Op::Tuple;
use crate::algebra::TupleOp::{Index, Input};
use crate::algebra::{
    Algebra, Algebraic, BoxedIterator, BoxedValueHandler, Op, Operator, ValueIterator,
};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::OutputType::Array;
use crate::processing::{ArrayType, Layout};
use crate::util::storage::ValueStore;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use value::Value;
use value::Value::Null;

type Agg = (AggOp, Operator);

/// Aggregate operations like SUM, COUNT, AVG
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Aggregate {
    pub input: Box<Algebraic>,
    pub aggregates: Vec<Agg>,
    output_func: Operator,
    group: Operator,
}

impl Aggregate {
    pub fn new(input: Box<Algebraic>, func: Operator, group: Option<Operator>) -> Self {
        let (output_func, aggregates) = extract_aggs(func);

        Aggregate {
            input,
            aggregates,
            group: group.unwrap_or(Operator::literal(Value::bool(true))),
            output_func,
        }
    }
}

fn extract_aggs(mut operator: Operator) -> (Operator, Vec<Agg>) {
    let mut aggregates = Vec::new();
    extract(&mut operator, &mut aggregates);
    (operator, aggregates)
}

fn extract(operator: &mut Operator, aggs: &mut Vec<Agg>) {
    match &operator.op {
        Op::Agg(a) => {
            let op = match operator.operands.len() {
                1 => operator.operands[0].clone(),
                _ => Operator::combine(operator.operands.clone()),
            };
            let i = aggs.len() + 1; // first is grouped
            aggs.push((a.clone(), op));
            // we replace with index operation on whole input
            operator.op = Op::Tuple(Index(IndexOp::new(i)));
            operator.operands = vec![Operator::input()]
        }
        Tuple(Input(_)) => {
            // we remove the additional inputs aggregates
            operator.op = Tuple(Index(IndexOp::new(0)));
            operator.operands = vec![Operator::input()]
        }
        _ => {
            operator.operands.iter_mut().for_each(|a| extract(a, aggs));
        }
    }
}

impl Hash for Aggregate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        state.write_usize(self.aggregates.len());
        self.group.hash(state);
    }
}

impl InputDerivable for Aggregate {
    fn derive_input_layout(&self) -> Option<Layout> {
        let ags = self
            .aggregates
            .iter()
            .map(|(op, ops)| {
                ops.derive_input_layout()
                    .unwrap_or_default()
                    .merge(&op.derive_input_layout(vec![]))
            })
            .fold(Layout::default(), |a, b| a.merge(&b));
        Some(self.group.derive_input_layout()?.merge(&ags))
    }
}

impl OutputDerivable for Aggregate {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        if self.aggregates.len() == 1 {
            let op = self.aggregates[0]
                .1
                .clone()
                .derive_output_layout(HashMap::new())?;
            Some(self.aggregates[0].0.derive_output_layout(vec![op], inputs))
        } else {
            Some(Layout::from(Array(Box::new(ArrayType::new(
                Layout::default(),
                Some(self.aggregates.len() as i32),
            )))))
        }
    }
}

impl Algebra for Aggregate {
    type Iterator = AggIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        let iter = self.input.derive_iterator();
        let group = self.group.implement().unwrap();
        let aggregates = self
            .aggregates
            .iter()
            .map(|(a, o)| (a.implement().unwrap(), o.implement().unwrap()))
            .collect();
        AggIterator::new(
            iter,
            aggregates,
            self.output_func.implement().unwrap(),
            group,
        )
    }
}

type AggHandler = (BoxedValueLoader, BoxedValueHandler);

pub struct AggIterator {
    input: BoxedIterator,
    groups: HashMap<u64, Vec<Value>>,
    hashes: HashMap<u64, Value>,
    output_func: BoxedValueHandler,
    values: Vec<Value>,
    hasher: BoxedValueHandler,
    aggregates: Vec<AggHandler>,
    reloaded: bool,
}

impl AggIterator {
    pub fn new(
        input: BoxedIterator,
        aggregates: Vec<AggHandler>,
        output_func: BoxedValueHandler,
        group: BoxedValueHandler,
    ) -> AggIterator {
        AggIterator {
            input,
            groups: Default::default(),
            hashes: Default::default(),
            output_func,
            values: vec![],
            hasher: group,
            aggregates,
            reloaded: false,
        }
    }

    pub(crate) fn reload_values(&mut self) {
        self.values.clear();
        self.groups.clear();
        self.hashes.clear();

        for value in self.input.by_ref() {
            let mut hasher = DefaultHasher::new();
            let keys = self.hasher.process(&value);

            keys.hash(&mut hasher);

            let hash = hasher.finish();

            self.hashes.entry(hash).or_insert(keys);
            self.groups.entry(hash).or_default().push(value);
        }

        for values in self.groups.values() {
            let mut aggregates = self
                .aggregates
                .iter()
                .map(|(agg, op)| ((*agg).clone(), (*op).clone()))
                .collect::<Vec<_>>();

            for value in values {
                for (ref mut agg, op) in &mut aggregates {
                    agg.load(&op.process(value))
                }
            }
            let mut end_values = vec![];
            // first we add grouped
            if !values.is_empty() {
                end_values.push(values[0].clone());
            } else {
                end_values.push(Null)
            }

            for (agg, _) in &aggregates {
                end_values.push(agg.get());
            }

            self.values
                .push(self.output_func.process(&end_values.into()));
        }

        self.reloaded = true;
    }
}

impl Iterator for AggIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(value) = self.values.pop() {
                return Some(value);
            } else if !self.reloaded {
                self.reload_values();
            } else {
                return None;
            }
        }
    }
}

impl ValueIterator for AggIterator {
    fn get_storages(&self) -> Vec<ValueStore> {
        self.input.get_storages()
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(AggIterator::new(
            self.input.clone(),
            self.aggregates
                .iter()
                .map(|(a, o)| ((*a).clone(), (*o).clone()))
                .collect(),
            self.output_func.clone(),
            self.hasher.clone(),
        ))
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        let input = self.input.enrich(transforms);

        if let Some(input) = input {
            self.input = input;
        };
        None
    }
}

pub trait ValueLoader {
    fn clone(&self) -> BoxedValueLoader;

    fn load(&mut self, value: &Value);

    fn get(&self) -> Value;
}

#[derive(Clone, Debug)]
pub struct CountOperator {
    count: usize,
}

impl CountOperator {
    pub fn new() -> Self {
        CountOperator { count: 0 }
    }
}

impl ValueLoader for CountOperator {
    fn clone(&self) -> BoxedValueLoader {
        Box::new(CountOperator::new())
    }

    fn load(&mut self, _value: &Value) {
        self.count += 1;
    }

    fn get(&self) -> Value {
        Value::int(self.count as i64)
    }
}

#[derive(Clone, Debug)]
pub struct SumOperator {
    sum: Value,
}

impl SumOperator {
    pub fn new() -> Self {
        SumOperator {
            sum: Value::float(0.0),
        }
    }
}

impl ValueLoader for SumOperator {
    fn clone(&self) -> BoxedValueLoader {
        Box::new(SumOperator::new())
    }

    fn load(&mut self, value: &Value) {
        self.sum += value.clone();
    }

    fn get(&self) -> Value {
        self.sum.clone()
    }
}

#[derive(Clone, Debug)]
pub struct AvgOperator {
    sum: Value,
    count: usize,
}

impl AvgOperator {
    pub fn new() -> Self {
        AvgOperator {
            sum: Value::float(0.0),
            count: 0,
        }
    }
}

impl ValueLoader for AvgOperator {
    fn clone(&self) -> BoxedValueLoader {
        Box::new(AvgOperator::new())
    }

    fn load(&mut self, value: &Value) {
        self.sum += value.clone();
        self.count += 1;
    }

    fn get(&self) -> Value {
        &self.sum.clone() / &Value::float(self.count as f64)
    }
}
