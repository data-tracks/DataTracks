use std::fmt::Formatter;

use crate::value::Value;
use crate::value::value::{ValType, Valuable};

#[derive(Eq, Hash, Clone, Debug, PartialEq)]
pub struct HoArray(Vec<Value>);

impl HoArray {
    pub fn new(values: Vec<Value>) -> Self {
        HoArray(values)
    }
}

impl Valuable for HoArray {
    fn type_(&self) -> ValType {
        ValType::Tuple
    }
}


impl From<Vec<Value>> for Value {

    fn from(value: Vec<Value>) -> Self {
        Value::array(value)
    }
}

impl std::fmt::Display for HoArray {
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