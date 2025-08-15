use crate::algebra::{
    AggOp, Algebra, AlgebraRoot, Algebraic, Filter, Op, Operator, Project, TupleOp,
};
use crate::optimize::rule::RuleBehavior;
use crate::util::CreatingVisitor;
use tracing::debug;
use value::Value;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum MergeRule {
    Filter,
    Project,
}

impl RuleBehavior for MergeRule {
    fn can_apply(&self, node_id: usize, root: &AlgebraRoot) -> bool {
        if let Some(alg) = root.get_node(node_id) {
            match_rule_with_child(self, alg, root)
        } else {
            false
        }
    }

    fn apply(&self, node_id: usize, root: &mut AlgebraRoot) -> Vec<Algebraic> {
        apply_rule_to_child(self, node_id, root);
        vec![]
    }
}

/// Match a specific rule with its child
fn match_rule_with_child(rule: &MergeRule, algebra: &Algebraic, root: &AlgebraRoot) -> bool {
    match (rule, algebra) {
        (MergeRule::Filter, Algebraic::Filter(f)) => match root.get_child(f.id()).unwrap() {
            Algebraic::Filter(other) => f.condition.can_merge(&other.condition),
            _ => false,
        },
        (MergeRule::Project, Algebraic::Project(p)) => match root.get_child(p.id()).unwrap() {
            Algebraic::Project(other) => p.project.can_merge(&other.project),
            _ => false,
        },
        _ => {
            debug!("{:?} {:?}", algebra, rule);
            false
        }
    }
}

/// Apply a specific rule to a child node
fn apply_rule_to_child(rule: &MergeRule, id: usize, root: &mut AlgebraRoot) {
    let node = match root.get_node(id) {
        None => return,
        Some(n) => n.clone(),
    };
    match (rule, node) {
        (MergeRule::Filter, Algebraic::Filter(f)) => {
            let child = if let Some(child) = root.get_child(f.id()) {
                child.clone()
            } else {
                return;
            };
            if let Algebraic::Filter(f_child) = child {
                let new_id = root.new_id();
                let alg = Algebraic::Filter(Filter::new(
                    new_id,
                    Operator::new(
                        Op::and(),
                        vec![f.condition.clone(), f_child.condition.clone()],
                    ),
                ));

                root.add_to_set(id, new_id);
                root.add_node(alg);

                if let Some(child) = root.get_child(f_child.id()) {
                    root.add_child(new_id, child.id())
                }
            }
        }
        (MergeRule::Project, Algebraic::Project(p)) => {
            let child = if let Some(child) = root.get_child(p.id()) {
                child.clone()
            } else {
                return;
            };

            if let Algebraic::Project(p_child) = child {
                let new_id = root.new_id();
                let alg = Algebraic::Project(Project::new(
                    new_id,
                    OperatorMerger::merge(&p_child.project, &mut p.project.clone()),
                ));

                root.add_to_set(id, new_id);
                root.add_node(alg);

                if let Some(child) = root.get_child(p_child.id()) {
                    root.add_child(new_id, child.id())
                }
            }
        }
        _ => {}
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
