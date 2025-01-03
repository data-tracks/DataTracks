use tracing::{info};
use crate::algebra::{AlgSet, AlgebraType};
use crate::optimize::rule::Rule::{Impossible, Merge};
use crate::optimize::rule::{Rule, RuleBehavior};
use crate::optimize::rules::MergeRule;
use crate::util::ChangingVisitor;

pub enum OptimizeStrategy {
    RuleBased(RuleBasedOptimizer),
}

impl OptimizeStrategy {
    pub(crate) fn apply(&mut self, raw: AlgebraType) -> AlgebraType {
        let expandable = AlgebraType::Set(add_set(raw));
        println!("alg {:?}", expandable);
        let optimized = match self {
            OptimizeStrategy::RuleBased(o) => {
                o.optimize(expandable.clone())}
        };
        remove_set(optimized.unwrap_or(expandable))
    }

    pub(crate) fn rule_based() -> Self {
        OptimizeStrategy::RuleBased(RuleBasedOptimizer::new())
    }
}

pub trait Optimizer {
    fn optimize(&mut self, raw: AlgebraType) -> Result<AlgebraType, String>;
}

pub struct RuleBasedOptimizer {
    rules: Vec<Rule>,
    current_rule: Rule,
}

impl RuleBasedOptimizer {
    pub fn new() -> Self {
        let mut rules: Vec<Rule> = Vec::new();

        rules.push(Merge(MergeRule::Filter));
        rules.push(Merge(MergeRule::Project));

        RuleBasedOptimizer {
            rules,
            current_rule: Impossible,
        }
    }
}

impl Optimizer for RuleBasedOptimizer {
    fn optimize(&mut self, raw: AlgebraType) -> Result<AlgebraType, String> {
        let rules = &self.rules.clone();
        let mut alg = raw.clone();
        let mut round = 0;
        let mut uneventful_rounds = 0;

        while uneventful_rounds < 2 {
            if round > rules.len() * 100 {
                return Err("Infinite loop detected".to_string());
            }

            let initial_cost = alg.calc_cost();

            for rule in rules {
                self.current_rule = rule.clone();
                self.visit(&mut alg)
            }

            if initial_cost <= alg.calc_cost() {
                uneventful_rounds += 1;
            } else {
                uneventful_rounds = 0;
            }

            round += 1;
        }
        info!("Used {} rounds for optimization.", round);
        Ok(alg)
    }
}

impl ChangingVisitor<&mut AlgebraType> for RuleBasedOptimizer {
    fn visit(&self, target: &mut AlgebraType) {
        match target {
            AlgebraType::Dual(_) => (),
            AlgebraType::IndexScan(_) => (),
            AlgebraType::TableScan(_) => (),
            AlgebraType::Project(ref mut p) => self.visit(&mut *p.input),
            AlgebraType::Filter(ref mut f) => self.visit(&mut *f.input),
            AlgebraType::Join(j) => {
                self.visit(&mut *j.left);
                self.visit(&mut *j.right);
            }
            AlgebraType::Union(u) => {
                u.inputs.iter_mut().for_each(|i| self.visit(i));
            }
            AlgebraType::Aggregate(a) => self.visit(&mut *a.input),
            AlgebraType::Variable(_) => (),
            AlgebraType::Set(ref mut s) => {

                if self.current_rule.can_apply(&AlgebraType::Set(s.clone())) {
                    let applied = self.current_rule.apply(&mut AlgebraType::Set(s.clone()));
                    s.set.extend(applied);
                }
            }
        }
    }
}

fn add_set(mut target: AlgebraType) -> AlgSet {
    match target {
        AlgebraType::Dual(_) => AlgSet::new(target),
        AlgebraType::IndexScan(_) => AlgSet::new(target),
        AlgebraType::TableScan(_) => AlgSet::new(target),
        AlgebraType::Project(ref mut p) => {
            p.input = Box::new(AlgebraType::Set(add_set((*p.input).clone())));
            AlgSet::new(target)
        }
        AlgebraType::Filter(ref mut f) => {
            f.input = Box::new(AlgebraType::Set(add_set((*f.input).clone())));
            AlgSet::new(target)
        }
        AlgebraType::Join(ref mut j) => {
            j.left = Box::new(AlgebraType::Set(add_set((*j.left).clone())));
            j.right = Box::new(AlgebraType::Set(add_set((*j.right).clone())));
            AlgSet::new(target)
        }
        AlgebraType::Union(ref mut u) => {
            u.inputs = u
                .inputs
                .iter()
                .map(|u| AlgebraType::Set(add_set(u.clone())))
                .collect();
            AlgSet::new(target)
        }
        AlgebraType::Aggregate(ref mut a) => {
            a.input = Box::new(AlgebraType::Set(add_set((*a.input).clone())));
            AlgSet::new(target)
        }
        AlgebraType::Variable(_) => AlgSet::new(target),
        AlgebraType::Set(mut s) => {
            s.initial = Box::new(AlgebraType::Set(add_set((*s.initial).clone())));
            s
        }
    }
}

fn remove_set(mut target: AlgebraType) -> AlgebraType {
    match &mut target {
        AlgebraType::Dual(_) => target,
        AlgebraType::IndexScan(_) => target,
        AlgebraType::TableScan(_) => target,
        AlgebraType::Project(ref mut p) => {
            p.input = Box::new(remove_set((*p.input).clone()));
            target
        }
        AlgebraType::Filter(ref mut f) => {
            f.input = Box::new(remove_set((*f.input).clone()));
            target
        }
        AlgebraType::Join(ref mut j) => {
            j.left = Box::new(remove_set((*j.left).clone()));
            j.right = Box::new(remove_set((*j.right).clone()));
            target
        }
        AlgebraType::Union(ref mut u) => {
            u.inputs = u.inputs.iter().map(|i| remove_set(i.clone())).collect();
            target
        }
        AlgebraType::Aggregate(ref mut a) => {
            a.input = Box::new(remove_set((*a.input).clone()));
            target
        }
        AlgebraType::Variable(_) => target,
        AlgebraType::Set(s) => {
            let mut best_cost = s.initial.calc_cost();
            let mut best = (*s.initial).clone();
            for a in &s.set {
                let cost = a.calc_cost();
                if cost < best_cost {
                    best = a.clone();
                    best_cost = cost;
                }
            }
            remove_set(best)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::algebra::{AlgebraType, Operator};
    use crate::optimize::optimizer::RuleBasedOptimizer;
    use crate::optimize::OptimizeStrategy;
    use std::vec;

    #[test]
    fn project_test() {
        let scan = AlgebraType::project(
            Operator::index(0, vec![Operator::input()]),
            AlgebraType::project(Operator::input(), AlgebraType::table("table".to_string())),
        );

        let optimizer = RuleBasedOptimizer::new();
        let optimized = OptimizeStrategy::RuleBased(optimizer).apply(scan);

        match optimized {
            AlgebraType::Project(p) => match p.input.as_ref() {
                AlgebraType::TableScan(_) => {}
                a => panic!("Expected project but got {:?}", a),
            },
            a => panic!("wrong algebra type {:?}", a),
        }
    }

    #[test]
    fn filter_test() {
        let scan = AlgebraType::filter(
            Operator::index(0, vec![Operator::input()]),
            AlgebraType::filter(Operator::input(), AlgebraType::table("table".to_string())),
        );

        let optimizer = RuleBasedOptimizer::new();
        let optimized = OptimizeStrategy::RuleBased(optimizer).apply(scan);

        match optimized {
            AlgebraType::Filter(ref p) => match p.input.as_ref() {
                AlgebraType::TableScan(_) => {}
                a => panic!("Expected filter but got {:?} in {:?}", a, optimized),
            },
            a => panic!("wrong algebra type {:?}", a),
        }
    }
}
