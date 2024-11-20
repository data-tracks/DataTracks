use std::cmp::Ordering;

pub struct  Effort {
    value: usize
}

impl Effort {
    pub(crate) fn default() -> Self {
        Effort{value:0}
    }
}

impl PartialEq<Self> for Effort {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl PartialOrd for Effort {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(&other.value)
    }
}
