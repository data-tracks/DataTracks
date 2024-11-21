use crate::algebra::AlgebraType;
use crate::optimize::rules::MergeRule;

#[derive(Debug, Clone)]
pub enum Rule {
    Merge(MergeRule)
}


pub trait RuleBehavior: Clone {
    fn can_apply(&self, algebra: &AlgebraType) -> bool;
    fn apply(&self, algebra: &AlgebraType) -> AlgebraType;
}

impl RuleBehavior for Rule {
    fn can_apply(&self, algebra: &AlgebraType) -> bool {
        match self {
            Rule::Merge(m) => m.can_apply(algebra),
        }
    }

    fn apply(&self, algebra: &AlgebraType) -> AlgebraType {
       match self {
           Rule::Merge(m) => m.apply(algebra),
       }
    }
}


