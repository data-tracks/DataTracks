use crate::algebra::{AggOp, Algebraic, Filter, Op, Operator, Project, TupleOp};
use crate::optimize::rule::RuleBehavior;
use crate::util::CreatingVisitor;
use value::Value;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum MergeRule {
    Filter,
    Project,
}

impl RuleBehavior for MergeRule {
    fn can_apply(&self, algebra: &Algebraic) -> bool {
        if let Algebraic::Set(s) = algebra {
            let bool = s.set.iter().any(|a| match_rule_with_child(self, a));
            bool
        } else {
            false
        }
    }

    fn apply(&self, algebra: &mut Algebraic) -> Vec<Algebraic> {
        if let Algebraic::Set(parent) = algebra {
            let values = parent
                .set
                .iter()
                .filter_map(|a| apply_rule_to_child(self, a))
                .collect();
            values
        } else {
            unreachable!("apply should only be called with AlgebraType::Set")
        }
    }
}

/// Match a specific rule with its child
fn match_rule_with_child(rule: &MergeRule, algebra: &Algebraic) -> bool {
    match (rule, algebra) {
        (MergeRule::Filter, Algebraic::Filter(f)) => {
            matches!(f.input.as_ref(), Algebraic::Set(s) if match &(*s.initial) {
                Algebraic::Filter(other) => {
                    f.condition.can_merge(&other.condition)
                },
                _ => false
            })
        }
        (MergeRule::Project, Algebraic::Project(p)) => {
            matches!(p.input.as_ref(), Algebraic::Set(s) if match &(*s.initial) {
                Algebraic::Project(other) => {
                    p.project.can_merge(&other.project)
                },
                _ => false
            })
        }
        _ => false,
    }
}

/// Apply a specific rule to a child node
fn apply_rule_to_child(rule: &MergeRule, algebra: &Algebraic) -> Option<Algebraic> {
    match (rule, algebra) {
        (MergeRule::Filter, Algebraic::Filter(f)) => {
            if let Algebraic::Set(parent) = f.input.as_ref() {
                parent
                    .set
                    .iter()
                    .filter_map(|b| match b {
                        Algebraic::Filter(f_child) => Some(Algebraic::Filter(Filter::new(
                            (*f_child.input).clone(),
                            Operator::new(
                                Op::and(),
                                vec![f.condition.clone(), f_child.condition.clone()],
                            ),
                        ))),
                        _ => None,
                    })
                    .next()
            } else {
                None
            }
        }
        (MergeRule::Project, Algebraic::Project(p)) => {
            if let Algebraic::Set(parent) = p.input.as_ref() {
                parent
                    .set
                    .iter()
                    .filter_map(|b| match b {
                        Algebraic::Project(p_child) => Some(Algebraic::Project(Project::new(
                            OperatorMerger::merge(&p_child.project, &mut p.project.clone()),
                            (*p_child.input).clone(),
                        ))),
                        _ => None,
                    })
                    .next()
            } else {
                None
            }
        }
        _ => None,
    }
}

struct OperatorMerger<'op> {
    child: &'op Operator,
}

impl OperatorMerger<'_> {
    fn merge(child: &Operator, parent: &mut Operator) -> Operator {
        let merger = OperatorMerger { child };
        merger.visit(parent)
    }
}

impl CreatingVisitor<&mut Operator, Operator> for OperatorMerger<'_> {
    fn visit(&self, parent: &mut Operator) -> Operator {
        match &parent.op {
            Op::Binary(_b) => {
                parent.operands = parent
                    .operands
                    .iter()
                    .cloned()
                    .map(|mut o| self.visit(&mut o))
                    .collect();
                parent.clone()
            }
            Op::Agg(AggOp::Count | AggOp::Sum | AggOp::Avg) => {
                parent.operands = parent
                    .operands
                    .iter()
                    .cloned()
                    .map(|mut o| self.visit(&mut o))
                    .collect();
                parent.clone()
            }
            Op::Collection(_) => {
                parent.operands = parent
                    .operands
                    .iter()
                    .cloned()
                    .map(|mut o| self.visit(&mut o))
                    .collect();
                parent.clone()
            }
            Op::Tuple(t) => match t {
                TupleOp::Name(n)
                    if parent.operands.len() == 1
                        && matches!(
                            parent.operands.first().unwrap().op,
                            Op::Tuple(TupleOp::Input(_))
                        ) =>
                {
                    self.handle_tuple_name(parent, &n.name)
                }
                TupleOp::Index(i)
                    if parent.operands.len() == 1
                        && matches!(
                            parent.operands.first().unwrap().op,
                            Op::Tuple(TupleOp::Input(_))
                        ) =>
                {
                    self.handle_tuple_index(parent, i.index)
                }
                _ => {
                    parent.operands = parent
                        .operands
                        .iter_mut()
                        .map(|mut o| self.visit(&mut o))
                        .collect();
                    parent.clone()
                }
            },
        }
    }
}

impl OperatorMerger<'_> {
    /// Handles the `TupleOp::Name` case.
    fn handle_tuple_name(&self, parent: &Operator, name: &str) -> Operator {
        match &self.child.op {
            Op::Tuple(TupleOp::Doc) => parent
                .operands
                .iter()
                .find_map(|o| match &o.op {
                    Op::Tuple(TupleOp::KeyValue(v)) if v.as_ref().is_some_and(|v| *v == name) => {
                        Some(o.clone())
                    }
                    _ => None,
                })
                .expect("KeyValue matching name not found"),
            Op::Tuple(TupleOp::Literal(l)) => match &l.literal {
                Value::Dict(d) => Operator::literal(d.get(name).expect("Key not found").clone()),
                Value::Wagon(w) => match w.value.as_ref() {
                    Value::Dict(d) => {
                        Operator::literal(d.get(name).expect("Key not found").clone())
                    }
                    _ => panic!("Unexpected Wagon value"),
                },
                _ => panic!("Unexpected Literal value"),
            },
            _ => panic!("Unsupported child operator for TupleOp::Name"),
        }
    }

    /// Handles the `TupleOp::Index` case.
    fn handle_tuple_index(&self, parent: &Operator, index: usize) -> Operator {
        match &self.child.op {
            Op::Tuple(TupleOp::Combine) => self
                .child
                .operands
                .get(index)
                .expect("Index out of bounds")
                .clone(),
            Op::Tuple(TupleOp::Doc) => {
                let child = self.child.operands.get(index).expect("Index out of bounds");
                match &child.op {
                    Op::Tuple(TupleOp::KeyValue(_)) => child
                        .operands
                        .first()
                        .expect("KeyValue missing child")
                        .clone(),
                    _ => panic!("Unexpected child operator for TupleOp::Doc"),
                }
            }
            Op::Tuple(TupleOp::Literal(l)) => match &l.literal {
                Value::Array(a) => {
                    Operator::literal(a.values.get(index).expect("Index out of bounds").clone())
                }
                Value::Dict(d) => Operator::literal(
                    d.values()
                        .nth(index)
                        .expect("Index out of bounds in Dict")
                        .clone(),
                ),
                Value::Wagon(w) => match w.value.as_ref() {
                    Value::Array(a) => {
                        Operator::literal(a.values.get(index).expect("Index out of bounds").clone())
                    }
                    Value::Dict(d) => Operator::literal(
                        d.values()
                            .nth(index)
                            .expect("Index out of bounds in Wagon Dict")
                            .clone(),
                    ),
                    _ => panic!("Unexpected Wagon value"),
                },
                _ => panic!("Unexpected Literal value"),
            },
            _ if index == 0 => self.child.clone(),
            _ => panic!(
                "Unsupported child operator for TupleOp::Index parent {:?} child {:?}",
                parent, self.child
            ),
        }
    }
}
