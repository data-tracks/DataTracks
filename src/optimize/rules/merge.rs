use crate::algebra::{AlgebraType, Filter, Op, Operator, Project, TupleOp};
use crate::algebra::TupleOp::And;
use crate::optimize::rule::RuleBehavior;


#[derive(Debug, Clone)]
pub enum MergeRule{
    Filter,
    Project
}


impl RuleBehavior for MergeRule {
    fn can_apply(&self, algebra: &AlgebraType) -> bool {
        match self {
            MergeRule::Filter => {
                match algebra {
                    AlgebraType::Set(s) => {
                        s.set.iter().any(|a| {
                            match a {
                                AlgebraType::Filter(f) => {
                                    match *f.input {
                                        AlgebraType::Set(s) => matches!(s.initial, MergeRule::Filter()),
                                        _ => false
                                    }
                                }
                                _ => false
                            }
                        })
                    }
                    _ => false
                }
            }
            MergeRule::Project => {
                match algebra {
                    AlgebraType::Set(s) => {
                        s.set.iter().any(|a| {
                            match a {
                                AlgebraType::Project(p) => {
                                    match *p.input {
                                        AlgebraType::Set(s) => matches!(s.initial, MergeRule::Project()),
                                        _ => false
                                    }
                                }
                                _ => false
                            }
                        })
                    }
                    _ => false
                }
            }
        }
    }

    fn apply(&self, algebra: &mut AlgebraType) -> Vec<AlgebraType> {
        match self {
            MergeRule::Filter => {
                match algebra {
                    AlgebraType::Set(parent) => {
                        let mut alternatives = vec![];
                        parent.set.iter().for_each(|a|{
                            match a {
                                AlgebraType::Filter(f) => {
                                    match *f.input {
                                        AlgebraType::Set(child) => {
                                            child.set.iter().for_each(|b| {
                                                match b {
                                                    AlgebraType::Filter(f_child) => {
                                                        let alg = AlgebraType::Filter(Filter::new((*f_child.input).clone(), Operator::new(Op::and(),  vec![f.condition, f_child.condition])));
                                                        alternatives.push(alg);
                                                    }
                                                    _ => {}
                                                }
                                            })
                                        }
                                        _ => {}
                                    }
                                }
                                _ => {}
                            }
                        });
                        alternatives
                    }
                    _ => unreachable!()
                }
            }
            MergeRule::Project => {
                match algebra {
                    AlgebraType::Set(parent) => {
                        let mut alternatives = vec![];
                        parent.set.iter().for_each(|a|{
                            match a {
                                AlgebraType::Project(p) => {
                                    match *p.input {
                                        AlgebraType::Set(child) => {
                                            child.set.iter().for_each(|b| {
                                                match b {
                                                    AlgebraType::Project(p_child) => {

                                                        let alg = AlgebraType::Project(Project::new((*p_child.input).clone(), Operator::new(Op::and(),  vec![p_child.project.chain(p.project)])));
                                                        alternatives.push(alg);
                                                    }
                                                    _ => {}
                                                }
                                            })
                                        }
                                        _ => {}
                                    }
                                }
                                _ => {}
                            }
                        });
                        alternatives
                    }
                    _ => unreachable!()
                }
            }
        }
    }
}