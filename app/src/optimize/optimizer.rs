use crate::algebra::{Algebra, AlgebraRoot, Algebraic};
use crate::optimize::rule::Rule::{Impossible, Merge};
use crate::optimize::rule::{Rule, RuleBehavior};
use crate::optimize::rules::MergeRule;
use crate::util::ChangingVisitor;
use tracing::debug;

pub enum OptimizeStrategy {
    RuleBased(RuleBasedOptimizer),
}

impl OptimizeStrategy {
    pub(crate) fn apply(&mut self, root: AlgebraRoot) -> Result<AlgebraRoot, String> {
        root.add_set();

        let root = match self {
            OptimizeStrategy::RuleBased(o) => o.optimize(root)?,
        };
        root.remove_set();
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
    rules: Vec<Rule>,
    current_rule: Rule,
}

impl Default for RuleBasedOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl RuleBasedOptimizer {
    pub fn new() -> Self {
        let rules: Vec<Rule> = vec![Merge(MergeRule::Project), Merge(MergeRule::Filter)];

        RuleBasedOptimizer {
            rules,
            current_rule: Impossible,
        }
    }
}

impl Optimizer for RuleBasedOptimizer {
    fn optimize(&mut self, root: AlgebraRoot) -> Result<AlgebraRoot, String> {
        let rules = &self.rules.clone();
        let mut root = root.clone();
        let mut round = 0;
        let mut uneventful_rounds = 0;

        while uneventful_rounds < 2 {
            if round > rules.len() * 100 {
                return Err("Infinite loop detected".to_string());
            }

            let initial_cost = root.calc_cost();

            for rule in rules {
                self.current_rule = rule.clone();
                for id in root.ends().clone() {
                    self.visit(id, &mut root)
                }
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
    fn visit(&self, target: usize, root: &mut AlgebraRoot) {
        match root.get_child(target).unwrap().clone() {
            Algebraic::Dual(_) => (),
            Algebraic::IndexScan(_) => (),
            Algebraic::Scan(_) => (),
            Algebraic::Project(p) => self.visit(root.get_child(p.id()).unwrap().id(), root),
            Algebraic::Filter(f) => self.visit(root.get_child(f.id()).unwrap().id(), root),
            Algebraic::Join(j) => {
                let children = root
                    .get_children(j.id())
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                self.visit(children.get(0).unwrap().id(), root);
                self.visit(children.get(1).unwrap().id(), root);
            }
            Algebraic::Union(u) => {
                let children = root
                    .get_children(u.id())
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                children.iter().for_each(|i| self.visit(i.id(), root));
            }
            Algebraic::Aggregate(a) => self.visit(root.get_child(a.id()).unwrap().id(), root),
            Algebraic::Variable(_) => (),
            Algebraic::Set(s) => {
                if self.current_rule.can_apply(s.id(), root) {
                    let applied = self.current_rule.apply(s.id(), root);
                    root.add_children(s.id(), applied);
                }
            }
            Algebraic::Sort(v) => self.visit(root.get_child(v.id()).unwrap().id(), root),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::algebra::{Algebra, AlgebraRoot, Algebraic, Operator};
    use crate::optimize::optimizer::RuleBasedOptimizer;
    use crate::optimize::OptimizeStrategy;
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
