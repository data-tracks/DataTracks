use crate::algebra::Algebraic;
use crate::optimize::rules::MergeRule;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Rule {
    Merge(MergeRule),
    Impossible,
}

pub trait RuleBehavior: Clone {
    fn can_apply(&self, algebra: &Algebraic) -> bool;
    fn apply(&self, algebra: &mut Algebraic) -> Vec<Algebraic>;
}

impl RuleBehavior for Rule {
    fn can_apply(&self, algebra: &Algebraic) -> bool {
        match self {
            Rule::Merge(m) => m.can_apply(algebra),
            Rule::Impossible => false,
        }
    }

    fn apply(&self, algebra: &mut Algebraic) -> Vec<Algebraic> {
        match self {
            Rule::Merge(m) => m.apply(algebra),
            Rule::Impossible => unreachable!(),
        }
    }
}
