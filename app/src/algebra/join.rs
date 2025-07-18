use crate::algebra::algebra::Algebra;
use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::{BoxedIterator, ValueIterator};
use crate::processing::OutputType::Array;
use crate::processing::transform::Transform;
use crate::processing::{ArrayType, Layout};
use crate::util::storage::ValueStore;
use std::collections::HashMap;
use value::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Join {
    id: usize,
    left_hash: fn(&Value) -> Value,
    right_hash: fn(&Value) -> Value,
    out: fn(Value, Value) -> Value,
}

impl Join {
    pub(crate) fn new(
        id: usize,
        left_hash: fn(&Value) -> Value,
        right_hash: fn(&Value) -> Value,
        out: fn(Value, Value) -> Value,
    ) -> Self {
        Join {
            id,
            left_hash,
            right_hash,
            out,
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

impl AlgInputDerivable for Join {
    fn derive_input_layout(&self, root: &AlgebraRoot) -> Option<Layout> {
        let children = root.get_children(self.id());
        let left = children.first().unwrap().derive_input_layout(root)?;
        let right = children.get(1).unwrap().derive_input_layout(root)?;
        Some(left.merge(&right))
    }
}

impl AlgOutputDerivable for Join {
    fn derive_output_layout(
        &self,
        inputs: HashMap<String, Layout>,
        root: &AlgebraRoot,
    ) -> Option<Layout> {
        let children = root.get_children(self.id());
        let left = children
            .first()
            .unwrap()
            .derive_output_layout(inputs.clone(), root)?;
        let right = children
            .get(1)
            .unwrap()
            .derive_output_layout(inputs, root)?;

        Some(Layout {
            type_: Array(Box::new(ArrayType::new(left.merge(&right), Some(2)))),
            ..Default::default()
        })
    }
}

impl Algebra for Join {
    type Iterator = JoinIterator;

    fn id(&self) -> usize {
        self.id
    }

    fn replace_id(self, id: usize) -> Self {
        Self { id, ..self }
    }

    fn derive_iterator(&self, root: &AlgebraRoot) -> Result<Self::Iterator, String> {
        let left_hash = self.left_hash.clone();
        let right_hash = self.right_hash.clone();
        let out = self.out.clone();

        let children = root.get_children(self.id());
        let left = children
            .get(0)
            .ok_or("Join has no left child.")?
            .derive_iterator(root)?;
        let right = children
            .get(1)
            .ok_or("Join has no right child.")?
            .derive_iterator(root)?;
        Ok(JoinIterator::new(left_hash, right_hash, out, left, right))
    }
}

#[cfg(test)]
mod test {
    use crate::algebra::AlgebraRoot;
    use value::{Dict, Value};

    #[test]
    fn one_match() {
        let left = transform(vec![3.into(), 5.5.into()]);
        let right = transform(vec![5.5.into(), "test".into()]);

        let mut root = AlgebraRoot::new_scan_index(0);

        root.scan_index(1);
        root.join_natural();

        let mut handle = root.derive_iterator().unwrap();

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

        let mut root = AlgebraRoot::new_scan_index(0);
        root.scan_index(1);
        root.join_natural();

        let mut handle = root.derive_iterator().unwrap();

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
