use crate::algebra::algebra::Algebra;
use crate::algebra::{Algebraic, BoxedIterator, ValueIterator};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::OutputType::Array;
use crate::processing::{ArrayType, Layout};
use crate::util::storage::ValueStore;
use std::collections::HashMap;
use value::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Join {
    pub left: Box<Algebraic>,
    pub right: Box<Algebraic>,
    left_hash: Option<fn(&Value) -> Value>,
    right_hash: Option<fn(&Value) -> Value>,
    out: Option<fn(Value, Value) -> Value>,
}

impl Join {
    pub(crate) fn new(
        left: Algebraic,
        right: Algebraic,
        left_hash: fn(&Value) -> Value,
        right_hash: fn(&Value) -> Value,
        out: fn(Value, Value) -> Value,
    ) -> Self {
        Join {
            left: Box::new(left),
            right: Box::new(right),
            left_hash: Some(left_hash),
            right_hash: Some(right_hash),
            out: Some(out),
        }
    }
}

pub struct JoinIterator {
    left_hash: fn(&Value) -> Value,
    right_hash: fn(&Value) -> Value,
    left: BoxedIterator,
    right: BoxedIterator,
    out: fn(Value, Value) -> Value,
    cache_left: Vec<(Value, Value)>,
    cache_right: Vec<(Value, Value)>,
    left_index: usize,
    right_index: usize,
}

impl JoinIterator {
    pub(crate) fn new(
        left_hash: fn(&Value) -> Value,
        right_hash: fn(&Value) -> Value,
        output: fn(Value, Value) -> Value,
        left: BoxedIterator,
        right: BoxedIterator,
    ) -> Self {
        JoinIterator {
            left_hash,
            right_hash,
            left,
            right,
            out: output,
            cache_left: vec![],
            cache_right: vec![],
            left_index: 0,
            right_index: 0,
        }
    }
}

impl Iterator for JoinIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        // only first time
        if self.cache_left.is_empty() && !self.next_left() {
            return None;
        }

        loop {
            if !self.next_right() {
                if !self.next_left() {
                    return None; // cannot advance further
                }
                self.right_index = 0;
            }

            let left = &self.cache_left.get(self.left_index)?;
            let right = &self.cache_right.get(self.right_index)?;

            if left.0 == right.0 {
                return Some((self.out)(left.1.clone(), right.1.clone()));
            }
        }
    }
}

impl JoinIterator {
    fn next_left(&mut self) -> bool {
        if let Some(val) = self.left.next() {
            self.cache_left.push(((self.left_hash)(&val), val));
            if self.cache_left.len() > 1 {
                self.left_index += 1;
            }
            self.right_index = 0; // we reset right
            true
        } else if self.left_index < self.cache_left.len() - 1 {
            self.left_index += 1;
            self.right_index = 0;

            true
        } else {
            false
        }
    }

    fn next_right(&mut self) -> bool {
        if let Some(val) = self.right.next() {
            self.cache_right.push(((self.right_hash)(&val), val));
            if self.cache_right.len() > 1 {
                self.right_index += 1;
            }
            true
        } else if self.cache_right.is_empty() {
            false
        } else if self.right_index < self.cache_right.len() - 1 {
            // index 0 length 1 cannot go further
            self.right_index += 1;

            true
        } else {
            false
        }
    }
}

impl ValueIterator for JoinIterator {
    fn get_storages(&self) -> Vec<ValueStore> {
        let mut left = self.left.get_storages();
        let right = self.right.get_storages();
        left.append(&mut right.to_vec());
        left
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(JoinIterator::new(
            self.left_hash,
            self.right_hash,
            self.out,
            self.left.clone(),
            self.right.clone(),
        ))
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        let left = self.left.enrich(transforms.clone());
        let right = self.right.enrich(transforms);

        if let Some(left) = left {
            self.left = left;
        }
        if let Some(right) = right {
            self.right = right;
        }
        None
    }
}

impl InputDerivable for Join {
    fn derive_input_layout(&self) -> Option<Layout> {
        let left = self.left.derive_input_layout()?;
        let right = self.right.derive_input_layout()?;
        Some(left.merge(&right))
    }
}

impl OutputDerivable for Join {
    fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Option<Layout> {
        let left = self.left.derive_output_layout(inputs.clone())?;
        let right = self.right.derive_output_layout(inputs)?;

        Some(Layout {
            type_: Array(Box::new(ArrayType::new(left.merge(&right), Some(2)))),
            ..Default::default()
        })
    }
}

impl Algebra for Join {
    type Iterator = JoinIterator;

    fn derive_iterator(&mut self) -> Self::Iterator {
        let left_hash = self.left_hash.take().unwrap();
        let right_hash = self.right_hash.take().unwrap();
        let out = self.out.take().unwrap();

        let left = self.left.derive_iterator();
        let right = self.right.derive_iterator();
        JoinIterator::new(left_hash, right_hash, out, left, right)
    }
}

#[cfg(test)]
mod test {
    use crate::algebra::algebra::Algebra;
    use crate::algebra::join::Join;
    use crate::algebra::scan::IndexScan;
    use crate::algebra::{Algebraic, ValueIterator};
    use value::{Dict, Value};

    #[test]
    fn one_match() {
        let left = transform(vec![3.into(), 5.5.into()]);
        let right = transform(vec![5.5.into(), "test".into()]);

        let left_scan = IndexScan::new(0);
        let right_scan = IndexScan::new(1);

        let mut join = Join::new(
            Algebraic::IndexScan(left_scan),
            Algebraic::IndexScan(right_scan),
            |val| val.clone(),
            |val| val.clone(),
            |left, right| Value::Dict(left.as_dict().unwrap().merge(right.as_dict().unwrap())),
        );

        let mut handle = join.derive_iterator();

        let storages = handle.get_storages();
        storages.iter().for_each(|val| {
            if val.index == 0 {
                val.append(left.clone())
            } else if val.index == 1 {
                val.append(right.clone())
            } else {
                panic!("Incorrect index")
            }
        });

        let res = handle.drain_to_train(3);
        assert_eq!(
            res.values,
            vec![Value::Dict(Dict::from(vec![5.5.into(), 5.5.into()]))]
        );
        assert_ne!(res.values, vec![Value::Dict(Dict::from(vec![]))]);
    }

    #[test]
    fn multi_match() {
        let left = transform(vec![3.into(), 5.5.into()]);
        let right = transform(vec![5.5.into(), 5.5.into()]);

        let left_scan = IndexScan::new(0);
        let right_scan = IndexScan::new(1);

        let mut join = Join::new(
            Algebraic::IndexScan(left_scan),
            Algebraic::IndexScan(right_scan),
            |val| val.clone(),
            |val| val.clone(),
            |left, right| Value::Dict(left.as_dict().unwrap().merge(right.as_dict().unwrap())),
        );

        let mut handle = join.derive_iterator();

        let storages = handle.get_storages();
        storages.iter().for_each(|val| {
            if val.index == 0 {
                val.append(left.clone())
            } else if val.index == 1 {
                val.append(right.clone())
            } else {
                panic!("Incorrect index")
            }
        });

        let res = handle.drain_to_train(3);
        assert_eq!(
            res.values,
            vec![
                Value::Dict(Dict::from(vec![5.5.into(), 5.5.into()])),
                Value::Dict(Dict::from(vec![5.5.into(), 5.5.into()]))
            ]
        );
        assert_ne!(res.values, vec![vec![].into()]);
    }

    pub fn transform(values: Vec<Value>) -> Vec<Value> {
        let mut dicts = vec![];
        for value in values {
            dicts.push(Value::Dict(value.into()));
        }
        dicts
    }
}
