use crate::util::reservoir::ValueReservoir;
use std::collections::HashMap;
use std::rc::Rc;
use value::Value;
use value::train::Train;

pub type BoxedValueIterator = Box<dyn ValueIterator<Item = Value> + Send + 'static>;

pub type BoxedValueHandler = Box<dyn ValueHandler + Send + Sync + 'static>;

pub type BoxedValueSplitter = Box<dyn Fn(Value) -> Vec<Value> + Send + 'static>;

pub type BoxedValueLoader = Box<dyn ValueLoader + Send + 'static>;

pub trait ValueLoader {
    fn clone_boxed(&self) -> BoxedValueLoader;

    fn load(&mut self, value: &Value);

    fn get(&self) -> Value;
}

pub struct EmptyIterator {}

impl Iterator for EmptyIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl ValueIterator for EmptyIterator {
    fn get_storages(&self) -> Vec<ValueReservoir> {
        vec![]
    }

    fn clone_boxed(&self) -> BoxedValueIterator {
        Box::new(EmptyIterator {})
    }

    fn enrich(
        &mut self,
        _transforms: Rc<HashMap<String, BoxedValueIterator>>,
    ) -> Option<BoxedValueIterator> {
        None
    }
}

pub trait ValueHandler: Send + 'static {
    fn process(&self, value: &Value) -> Value;

    fn clone_boxed(&self) -> BoxedValueHandler;
}

impl Clone for Box<dyn ValueHandler> {
    fn clone(&self) -> Self {
        self.clone_boxed()
    }
}

#[derive(Clone)]
pub struct IdentityHandler;

impl IdentityHandler {
    pub fn new_boxed() -> BoxedValueHandler {
        Box::new(IdentityHandler {})
    }
}
impl ValueHandler for IdentityHandler {
    fn process(&self, value: &Value) -> Value {
        value.clone()
    }
    fn clone_boxed(&self) -> BoxedValueHandler {
        Box::new(IdentityHandler)
    }
}

pub trait ValueIterator: Iterator<Item = Value> + Send + 'static {
    fn get_storages(&self) -> Vec<ValueReservoir>;

    fn drain(&mut self) -> Vec<Value> {
        self.into_iter().collect()
    }

    fn drain_to_train(&mut self, stop: usize) -> Train {
        Train::new_values(self.drain(), 0, 0).mark(stop)
    }

    fn clone_boxed(&self) -> BoxedValueIterator;

    fn enrich(
        &mut self,
        transforms: Rc<HashMap<String, BoxedValueIterator>>,
    ) -> Option<BoxedValueIterator>;
}
