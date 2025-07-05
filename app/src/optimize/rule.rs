use crate::algebra::{AlgebraRoot, Algebraic};
use crate::optimize::rules::MergeRule;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Rule {
    Merge(MergeRule),
    Impossible,
}

pub trait RuleBehavior: Clone {
    fn can_apply(&self, algebra: usize, root: &AlgebraRoot) -> bool;
    fn apply(&self, algebra: usize, root: &mut AlgebraRoot) -> Vec<Algebraic>;
}

impl RuleBehavior for Rule {
    fn can_apply(&self, alg_id: usize, root: &AlgebraRoot) -> bool {
        match self {
            Rule::Merge(m) => m.can_apply(alg_id, root),
            Rule::Impossible => false,
        }
    }

    fn apply(&self, alg_id: usize, root: &mut AlgebraRoot) -> Vec<Algebraic> {
        match self {
            Rule::Merge(m) => m.apply(alg_id, root),
            Rule::Impossible => unreachable!(),
        }
    }
}
