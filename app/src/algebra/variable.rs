use crate::algebra::root::{AlgInputDerivable, AlgOutputDerivable, AlgebraRoot};
use crate::algebra::Algebra;
use crate::processing::Layout;
use core::util::reservoir::ValueReservoir;
use core::{BoxedValueIterator, ValueIterator};
use std::collections::HashMap;
use std::rc::Rc;
use value::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct VariableScan {
    id: usize,
    name: String,
}

impl VariableScan {
    pub(crate) fn new(id: usize, name: String) -> Self {
        VariableScan { id, name }
    }
}

impl AlgInputDerivable for VariableScan {
    fn derive_input_layout(&self, _root: &AlgebraRoot) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl AlgOutputDerivable for VariableScan {
    fn derive_output_layout(
        &self,
        _inputs: HashMap<String, Layout>,
        _root: &AlgebraRoot,
    ) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl Algebra for VariableScan {
    type Iterator = BareVariableIterator;

    fn id(&self) -> usize {
        self.id
    }

    fn replace_id(self, id: usize) -> Self {
        Self { id, ..self }
    }

    fn derive_iterator(&self, root: &AlgebraRoot) -> Result<Self::Iterator, String> {
        Ok(BareVariableIterator::new(
            self.name.clone(),
            root.get_children(self.id())
                .iter()
                .map(|i| i.derive_iterator(root))
                .collect::<Result<Vec<_>, String>>()?,
        ))
    }
}

pub struct BareVariableIterator {
    name: String,
    inputs: Vec<BoxedValueIterator>,
}

impl BareVariableIterator {
    pub(crate) fn new(name: String, inputs: Vec<BoxedValueIterator>) -> Self {
        BareVariableIterator { name, inputs }
    }
}

impl Iterator for BareVariableIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        panic!("Not correctly enriched")
    }
}

impl ValueIterator for BareVariableIterator {
    fn get_storages(&self) -> Vec<ValueReservoir> {
        unreachable!("Not correctly enriched")
    }

    fn clone_boxed(&self) -> BoxedValueIterator {
        Box::new(BareVariableIterator {
            name: self.name.clone(),
            inputs: self.inputs.iter().map(|i| (*i).clone_boxed()).collect(),
        })
    }

    fn enrich(&mut self, transforms: Rc<HashMap<String, BoxedValueIterator>>) -> Option<BoxedValueIterator> {
        let transform = transforms.get(&self.name).unwrap();
        let name = self.name.clone();
        Some(Box::new(VariableIterator::new(
            name.into(),
            self.inputs.iter().map(|v| v.clone_boxed()).collect(),
            transform.clone_boxed(),
        )))
    }
}

pub struct VariableIterator {
    transform: BoxedValueIterator,
    inputs: Vec<BoxedValueIterator>,
    store: ValueReservoir,
    name: Value,
}

impl VariableIterator {
    pub(crate) fn new(name: Value, inputs: Vec<BoxedValueIterator>, transform: BoxedValueIterator) -> Self {
        let store = transform.get_storages().pop().unwrap();

        VariableIterator {
            inputs,
            transform,
            name,
            store,
        }
    }
}

impl Iterator for VariableIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        let value = {
            let value = self.transform.next();

            if let Some(value) = value {
                return Some(value);
            }

            let values: Vec<_> = self.inputs.iter_mut().map(|v| v.next()).collect();
            if values.iter().any(|v| v.is_none()) {
                return None;
            }
            let values = values.iter().map(|v| v.clone().unwrap()).collect();

            self.store.append(values);

            self.transform.next()
        };
        // we annotate it
        if let Some(value) = value {
            Some(Value::wagon(value, self.name.clone()))
        } else {
            None
        }
    }
}

impl ValueIterator for VariableIterator {
    fn get_storages(&self) -> Vec<ValueReservoir> {
        self.inputs
            .iter()
            .map(|v| v.get_storages())
            .reduce(|mut a, mut b| {
                a.append(&mut b);
                a
            })
            .unwrap()
    }

    fn clone_boxed(&self) -> BoxedValueIterator {
        Box::new(VariableIterator::new(
            self.name.clone(),
            self.inputs.iter().map(|v| (*v).clone_boxed()).collect(),
            self.transform.clone_boxed(),
        ))
    }

    fn enrich(&mut self, _transforms: Rc<HashMap<String, BoxedValueIterator>>) -> Option<BoxedValueIterator> {
        None
    }
}
