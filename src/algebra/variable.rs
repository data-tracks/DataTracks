use crate::algebra::{Algebra, AlgebraType, BoxedIterator, ValueIterator};
use crate::analyse::{InputDerivable, OutputDerivable};
use crate::processing::transform::Transform;
use crate::processing::{Layout, Train};
use crate::value::Value;
use std::collections::HashMap;
use std::vec;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct VariableScan {
    name: String,
    inputs: Vec<AlgebraType>,
}

impl VariableScan {
    pub(crate) fn new(name: String, inputs: Vec<AlgebraType>) -> Self {
        VariableScan { name, inputs }
    }
}

impl InputDerivable for VariableScan {
    fn derive_input_layout(&self) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl OutputDerivable for VariableScan {
    fn derive_output_layout(&self, _inputs: HashMap<String, &Layout>) -> Option<Layout> {
        Some(Layout::default())
    }
}

impl Algebra for VariableScan {
    type Iterator = BareVariableIterator;
    fn derive_iterator(&mut self) -> Self::Iterator {
        BareVariableIterator::new(self.name.clone(), self.inputs.iter_mut().map(|i| i.derive_iterator()).collect())
    }

}

pub struct BareVariableIterator {
    name: String,
    inputs: Vec<BoxedIterator>,
}

impl BareVariableIterator {
    pub(crate) fn new(name: String, inputs: Vec<BoxedIterator>) -> Self {
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
    fn dynamically_load(&mut self, _trains: Vec<Train>) {
        panic!("Not correctly enriched")
    }
    fn clone(&self) -> BoxedIterator {
        Box::new(BareVariableIterator { name: self.name.clone(), inputs: self.inputs.iter().map(|i| (*i).clone()).collect() })
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        let transform = transforms.get(&self.name).unwrap();
        let name = self.name.clone();
        Some(Box::new(VariableIterator::new(name, self.inputs.iter().map(|v| (*v).clone()).collect(), transform.optimize(transforms.clone(), None))))
    }
}

pub struct VariableIterator {
    transform: BoxedIterator,
    inputs: Vec<BoxedIterator>,
    name: String,
}

impl VariableIterator {
    pub(crate) fn new(name: String, inputs: Vec<BoxedIterator>, transform: BoxedIterator) -> Self {
        VariableIterator { inputs, transform, name }
    }
}

impl Iterator for VariableIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        let value = {
            let value = self.transform.next();

            if let Some(value) = value {
                return Some(value)
            }

            let values: Vec<_> = self.inputs.iter_mut().map(|v| v.next()).collect();
            if values.iter().any(|v| v.is_none()) {
                return None;
            }
            let values = values.iter().map(|v| v.clone().unwrap()).collect();

            self.transform.dynamically_load(vec![Train::new(values)]);
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
    fn dynamically_load(&mut self, trains: Vec<Train>) {
        self.inputs.iter_mut().for_each(|v| v.dynamically_load(trains.clone()));
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(VariableIterator::new(self.name.clone(), self.inputs.iter().map(|v| (*v).clone()).collect(), self.transform.clone()))
    }

    fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        None
    }
}