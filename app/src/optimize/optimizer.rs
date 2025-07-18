use crate::algebra::{AlgSet, Algebra, AlgebraRoot, Algebraic};
use crate::optimize::rule::Rule::{Impossible, Merge};
use crate::optimize::rule::{Rule, RuleBehavior};
use crate::optimize::rules::MergeRule;
use crate::util::ChangingVisitor;
use std::collections::{HashMap, HashSet};
use tracing::debug;

pub enum OptimizeStrategy {
    RuleBased(RuleBasedOptimizer),
}

impl OptimizeStrategy {
    pub(crate) fn apply(&mut self, root: AlgebraRoot) -> Result<AlgebraRoot, String> {
        let root = match self {
            OptimizeStrategy::RuleBased(o) => o.optimize(root)?,
        };

        Ok(root)
    }

    pub(crate) fn rule_based() -> Self {
        OptimizeStrategy::RuleBased(RuleBasedOptimizer::new())
    }
}

pub trait Optimizer {
    fn optimize(&mut self, root: AlgebraRoot) -> Result<AlgebraRoot, String>;
}

pub struct RuleBasedOptimizer {
    current_rule: Rule,
    visited_sets: HashSet<usize>,
    current_applied: Vec<usize>,
    rules: HashMap<Rule, Vec<usize>>,
}

impl Default for RuleBasedOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl RuleBasedOptimizer {
    pub fn new() -> Self {
        let rules = vec![Merge(MergeRule::Project), Merge(MergeRule::Filter)]
            .into_iter()
            .map(|r| (r, Vec::new()))
            .collect();

        RuleBasedOptimizer {
            rules,
            current_rule: Impossible,
            visited_sets: Default::default(),
            current_applied: Default::default(),
        }
    }
}

impl Optimizer for RuleBasedOptimizer {
    fn optimize(&mut self, root: AlgebraRoot) -> Result<AlgebraRoot, String> {
        //let rules = &self.rules.clone();
        let mut root = root.clone();
        let mut round = 0;
        let mut uneventful_rounds = 0;

        let rules = self.rules.keys().cloned().collect::<Vec<_>>();

        while uneventful_rounds < 2 {
            if round > self.rules.len() * 100 {
                return Err("Infinite loop detected".to_string());
            }

            let initial_cost = root.calc_cost();

            for rule in &rules {
                self.current_rule = rule.clone();
                // we reset what we visited due to new rule
                self.visited_sets.clear();
                self.current_applied.clear();

                for id in root.ends().clone() {
                    <RuleBasedOptimizer as ChangingVisitor<&mut Algebraic>>::visit(
                        self, id, &mut root,
                    )
                }
                let mut values = std::mem::take(&mut self.current_applied);
                self.rules.get_mut(rule).unwrap().append(&mut values);
            }

            if initial_cost <= root.calc_cost() {
                uneventful_rounds += 1;
            } else {
                uneventful_rounds = 0;
            }

            round += 1;
        }
        debug!("Used {} rounds for optimization.", round);
        Ok(root)
    }
}

impl ChangingVisitor<&mut Algebraic> for RuleBasedOptimizer {
    fn visit(&mut self, target: usize, root: &mut AlgebraRoot) {
        if self.visited_sets.contains(&target) {
            // we have already been here
            return;
        }

        // try applying the rule
        <RuleBasedOptimizer as ChangingVisitor<&mut AlgSet>>::visit(self, target, root);

        let alternatives = root
            .get_set(target)
            .map(|alg| alg.alternatives.clone())
            .unwrap_or_default();

        for id in alternatives {
            if id == target {
                // current node
                continue;
            }

            // hand the rule to all alternatives
            <RuleBasedOptimizer as ChangingVisitor<&mut Algebraic>>::visit(self, id, root);
        }

        if let Some(alg) = root.get_node(target) {
            match alg.clone() {
                Algebraic::Dual(_) => (),
                Algebraic::IndexScan(_) => (),
                Algebraic::Scan(_) => (),
                Algebraic::Project(p) => {
                    <RuleBasedOptimizer as ChangingVisitor<&mut Algebraic>>::visit(
                        self,
                        root.get_child(p.id()).unwrap().id(),
                        root,
                    )
                }
                Algebraic::Filter(f) => {
                    <RuleBasedOptimizer as ChangingVisitor<&mut Algebraic>>::visit(
                        self,
                        root.get_child(f.id()).unwrap().id(),
                        root,
                    )
                }
                Algebraic::Join(j) => {
                    let children = root
                        .get_children(j.id())
                        .into_iter()
                        .cloned()
                        .collect::<Vec<_>>();
                    <RuleBasedOptimizer as ChangingVisitor<&mut Algebraic>>::visit(
                        self,
                        children.get(0).unwrap().id(),
                        root,
                    );
                    <RuleBasedOptimizer as ChangingVisitor<&mut Algebraic>>::visit(
                        self,
                        children.get(1).unwrap().id(),
                        root,
                    );
                }
                Algebraic::Union(u) => {
                    let children = root
                        .get_children(u.id())
                        .into_iter()
                        .cloned()
                        .collect::<Vec<_>>();
                    children.iter().for_each(|i| {
                        <RuleBasedOptimizer as ChangingVisitor<&mut Algebraic>>::visit(
                            self,
                            i.id(),
                            root,
                        )
                    });
                }
                Algebraic::Aggregate(a) => <RuleBasedOptimizer as ChangingVisitor<
                    &mut Algebraic,
                >>::visit(
                    self, root.get_child(a.id()).unwrap().id(), root
                ),
                Algebraic::Variable(_) => (),
                Algebraic::Sort(v) => {
                    <RuleBasedOptimizer as ChangingVisitor<&mut Algebraic>>::visit(
                        self,
                        root.get_child(v.id()).unwrap().id(),
                        root,
                    )
                }
            }
        }
    }
}

impl ChangingVisitor<&mut AlgSet> for RuleBasedOptimizer {
    fn visit(&mut self, target: usize, root: &mut AlgebraRoot) {
        if self.visited_sets.contains(&target) || self.current_applied.contains(&target) {
            // already applied
            return;
        }

        if self.current_rule.can_apply(target, root) {
            self.current_rule.apply(target, root);

            self.current_applied.push(target);
        } else {
            self.visited_sets.insert(target);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::algebra::{Algebra, AlgebraRoot, Algebraic, Operator};
    use crate::optimize::OptimizeStrategy;
    use crate::optimize::optimizer::RuleBasedOptimizer;
    use std::vec;

    #[test]
    fn project_test() {
        let mut root = AlgebraRoot::new_scan_name("table");
        root.project(Operator::input());
        root.project(Operator::index(0, vec![Operator::input()]));

        let optimizer = RuleBasedOptimizer::new();
        let mut optimized = OptimizeStrategy::RuleBased(optimizer).apply(root).unwrap();

        match optimized.pop() {
            Ok(Algebraic::Project(p)) => match optimized.get_child(p.id()) {
                Some(Algebraic::Scan(_)) => {}
                a => panic!("Expected project but got {:?}", a),
            },
            a => panic!("wrong algebra type {:?}", a),
        }
    }

    #[test]
    fn filter_test() {
        let mut root = AlgebraRoot::new_scan_name("table");
        root.filter(Operator::input());
        root.filter(Operator::index(0, vec![Operator::input()]));

        let optimizer = RuleBasedOptimizer::new();
        let mut optimized = OptimizeStrategy::RuleBased(optimizer).apply(root).unwrap();

        match optimized.pop() {
            Ok(Algebraic::Filter(p)) => match optimized.get_child(p.id()) {
                Some(Algebraic::Scan(_)) => {}
                a => panic!("Expected filter but got {:?} in {:?}", a, optimized),
            },
            a => panic!("wrong algebra type {:?}", a),
        }
    }
}
