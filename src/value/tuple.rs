use std::fmt::Formatter;

use crate::value::Value;
use crate::value::value::{ValType, Valuable};

#[derive(Clone, Debug)]
pub struct HoTuple(Vec<Value>);

impl HoTuple {
    pub fn new(values: Vec<Value>) -> Self {
        HoTuple(values)
    }
}

impl Valuable for HoTuple {
    fn type_(&self) -> ValType {
        ValType::Tuple
    }
}

impl PartialEq for HoTuple {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Into<Value> for Vec<Value> {
    fn into(self) -> Value {
        Value::tuple(self)
    }
}

impl std::fmt::Display for HoTuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

#[cfg(test)]
mod test {
    use crate::value::Value;

    #[test]
    fn nested_eq_list() {
        let comps: [Value; 2] = [
            vec![3.into(), 2.into()].into(),
            vec![3.into(), vec![3.into(), 3.into()].into()].into()
        ];

        for comp in comps {
            assert_eq!(comp, comp)
        }
    }

    #[test]
    fn nested_ne_list() {
        let comps: [(Value, Value); 2] = [
            (vec![3.into(), 2.into()].into(), vec![3.into(), 3.into()].into()),
            (vec![3.into(), vec![3.into(), 3.into()].into()].into(), vec![3.into(), vec![3.into(), 1.into()].into()].into())
        ];

        for (left, right) in comps {
            assert_ne!(left, right)
        }
    }
}