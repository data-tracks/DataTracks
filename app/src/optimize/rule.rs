use crate::algebra::AlgebraType;
use crate::optimize::rules::MergeRule;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Rule {
    Merge(MergeRule),
    Impossible,
}

pub trait RuleBehavior: Clone {
    fn can_apply(&self, algebra: &AlgebraType) -> bool;
    fn apply(&self, algebra: &mut AlgebraType) -> Vec<AlgebraType>;
}

impl RuleBehavior for Rule {
    fn can_apply(&self, algebra: &AlgebraType) -> bool {
        match self {
            Rule::Merge(m) => m.can_apply(algebra),
            Rule::Impossible => false,
        }
    }

    fn apply(&self, algebra: &mut AlgebraType) -> Vec<AlgebraType> {
        match self {
            Rule::Merge(m) => m.apply(algebra),
            Rule::Impossible => unreachable!(),
        }
    }
}
