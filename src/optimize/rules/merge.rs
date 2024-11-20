use crate::algebra::AlgebraType;
use crate::optimize::rule::RuleBehavior;


#[derive(Debug, Clone)]
pub enum MergeRule{
    Filter,
    Project
}


impl RuleBehavior for MergeRule {
    fn can_apply(&self, algebra: &AlgebraType) -> bool {
        todo!()
    }

    fn apply(&self, algebra: &AlgebraType) -> AlgebraType {
        todo!()
    }
}