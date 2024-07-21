use std::collections::BTreeMap;
use std::hash::Hash;

use crate::value::{ValType, Value};
use crate::value::value::Valuable;

#[derive(Eq, Clone, Debug, Hash, PartialEq, Default)]
pub struct HoDict(BTreeMap<String, Value>);

impl HoDict{
    pub fn new(values: BTreeMap<String, Value>) -> Self{
        HoDict(values.into())
    }
}

impl Valuable for HoDict {
    fn type_(&self) -> ValType {
        ValType::Dict
    }
}