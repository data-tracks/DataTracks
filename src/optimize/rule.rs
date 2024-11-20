use crate::algebra::AlgebraType;
use crate::optimize::rules::MergeRule;

pub enum Rule {
    Merge(MergeRule)
}



pub trait RuleBehavior: Clone {
    fn can_apply(&self, algebra: &AlgebraType) -> bool;
    fn apply(&self, algebra: &AlgebraType) -> AlgebraType;
}


