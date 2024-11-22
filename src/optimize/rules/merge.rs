use crate::algebra::{AggOp, AlgebraType, Filter, Op, Operator, Project, TupleOp};
use crate::optimize::rule::RuleBehavior;
use crate::util::CreatingVisitor;
use crate::value::Value;

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
                                    match f.input.as_ref() {
                                        AlgebraType::Set(s) => matches!(*s.initial, AlgebraType::Filter(..)),
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
                                    match p.input.as_ref() {
                                        AlgebraType::Set(s) => matches!(*s.initial, AlgebraType::Project(..)),
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
                                    match f.input.as_ref() {
                                        AlgebraType::Set(child) => {
                                            child.set.iter().for_each(|b| {
                                                match b {
                                                    AlgebraType::Filter(f_child) => {
                                                        let alg = AlgebraType::Filter(Filter::new((*f_child.input).clone(), Operator::new(Op::and(),  vec![f.condition.clone(), f_child.condition.clone()])));
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
                        parent.set.iter_mut().for_each(|a|{
                            match a {
                                AlgebraType::Project(ref mut p) => {
                                    match p.input.as_ref() {
                                        AlgebraType::Set(child) => {
                                            child.set.iter().for_each(|b| {
                                                match b {
                                                    AlgebraType::Project(p_child) => {

                                                        let alg = AlgebraType::Project(Project::new((*p_child.input).clone(), Operator::new(Op::and(),  vec![OperatorMerger::merge(&p_child.project, &mut p.project)])));
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


struct OperatorMerger<'op>{
    child: &'op Operator,
}

impl OperatorMerger<'_>{
    fn merge(child: &Operator, parent: &mut Operator ) -> Operator {
        let merger = OperatorMerger{child};
        merger.visit(parent)
    }
}

impl CreatingVisitor<&mut Operator, Operator> for OperatorMerger<'_>{
    fn visit(&self, parent: &mut Operator) -> Operator {
        match &parent.op {
            Op::Agg(AggOp::Count | AggOp::Sum | AggOp::Avg) => {
                parent.operands = parent.operands.iter().cloned().map(|mut o| self.visit(&mut o)).collect();
                parent.clone()
            }
            Op::Tuple(t) => {
                match t {
                    TupleOp::Name(n) => {
                        match &self.child.op {
                            Op::Tuple(t) => {
                                match t {
                                    TupleOp::Doc => {
                                        let value = parent.operands.iter().filter(|o| {
                                            match &o.op {
                                                Op::Tuple(TupleOp::KeyValue(v)) => v.as_ref().is_some_and(|v| *v == n.name),
                                                _ => panic!()
                                            }
                                        } ).cloned().collect::<Vec<_>>().first().cloned().unwrap();
                                        value
                                    },
                                    TupleOp::Literal(l) => {
                                        match &l.literal {
                                            Value::Dict(d)  => {
                                                Operator::literal(d.get(&n.name).unwrap().clone())
                                            }
                                            Value::Wagon( w) => {
                                                match w.value.as_ref() {
                                                    Value::Dict(d) => {
                                                        Operator::literal(d.get(&n.name).unwrap().clone())
                                                    }
                                                    _ => panic!()
                                                }
                                            }
                                            _ => panic!()
                                        }
                                    }
                                    _ => panic!()
                                }
                            }
                            _ => panic!()
                        }
                    }
                    TupleOp::Index(i) => {
                        match &self.child.op {
                            Op::Tuple(t) => {
                                match t {
                                    TupleOp::Combine => {
                                        self.child.operands.get(i.index).unwrap().clone()
                                    }
                                    TupleOp::Doc => {
                                        let child = self.child.operands.get(i.index).unwrap();
                                        match &child.op {
                                            Op::Tuple(TupleOp::KeyValue(_)) => child.operands.get(0).cloned().unwrap(),
                                            _ => panic!()
                                        }
                                    }
                                    TupleOp::Literal(l) => {
                                        match &l.literal {
                                            Value::Array(a)  => {
                                                Operator::literal(a.0.get(i.index).unwrap().clone())
                                            }
                                            Value::Dict(d)  => {
                                                Operator::literal(d.values().nth(i.index).unwrap().clone())
                                            }
                                            Value::Wagon(w) => {
                                                match w.value.as_ref() {
                                                    Value::Array(a) => {
                                                        Operator::literal(a.0.get(i.index).unwrap().clone())
                                                    }
                                                    Value::Dict(d) => {
                                                        Operator::literal(d.values().nth(i.index).unwrap().clone())
                                                    }
                                                    _ => panic!()
                                                }
                                            }
                                            _ => panic!()
                                        }
                                    }
                                    _ => panic!()
                                }
                            },
                            _ => panic!()
                        }
                    }
                    _ => {
                        parent.operands = parent.operands.iter_mut().map(|mut o| self.visit(&mut o)).collect();
                        parent.clone()
                    }
                }
            }
        }
    }
}