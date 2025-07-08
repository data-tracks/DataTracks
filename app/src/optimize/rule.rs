use crate::algebra::{AlgebraRoot, Algebraic};
use crate::optimize::rules::MergeRule;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Rule {
    Merge(MergeRule),
    Impossible,
}

pub trait RuleBehavior: Clone {
    fn can_apply(&self, node_id: usize, root: &AlgebraRoot) -> bool;
    fn apply(&self, node_id: usize, root: &mut AlgebraRoot) -> Vec<Algebraic>;
}

impl RuleBehavior for Rule {
    fn can_apply(&self, set_id: usize, root: &AlgebraRoot) -> bool {
        match self {
            Rule::Merge(m) => m.can_apply(set_id, root),
            Rule::Impossible => false,
        }
    }

    fn apply(&self, set_id: usize, root: &mut AlgebraRoot) -> Vec<Algebraic> {
        match self {
            Rule::Merge(m) => m.apply(set_id, root),
            Rule::Impossible => unreachable!(),
        }
    }
}
