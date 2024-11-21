use std::cmp::Ordering;

pub struct Reward {
    value: usize
}

impl Reward {
    pub(crate) fn default() -> Self {
        Reward {value:0}
    }
}

impl PartialEq<Self> for Reward {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl PartialOrd for Reward {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(&other.value)
    }
}
