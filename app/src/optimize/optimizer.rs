use crate::algebra::{AlgSet, Algebraic};
use crate::optimize::rule::Rule::{Impossible, Merge};
use crate::optimize::rule::{Rule, RuleBehavior};
use crate::optimize::rules::MergeRule;
use crate::util::ChangingVisitor;
use tracing::debug;

pub enum OptimizeStrategy {
    RuleBased(RuleBasedOptimizer),
}

impl OptimizeStrategy {
    pub(crate) fn apply(&mut self, raw: Algebraic) -> Algebraic {
        let expandable = Algebraic::Set(add_set(raw));
        let optimized = match self {
            OptimizeStrategy::RuleBased(o) => o.optimize(expandable.clone()),
        };
        remove_set(optimized.unwrap_or(expandable))
    }

    pub(crate) fn rule_based() -> Self {
        OptimizeStrategy::RuleBased(RuleBasedOptimizer::new())
    }
}

pub trait Optimizer {
    fn optimize(&mut self, raw: Algebraic) -> Result<Algebraic, String>;
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
    fn optimize(&mut self, raw: Algebraic) -> Result<Algebraic, String> {
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
        debug!("Used {} rounds for optimization.", round);
        Ok(alg)
    }
}

impl ChangingVisitor<&mut Algebraic> for RuleBasedOptimizer {
    fn visit(&self, target: &mut Algebraic) {
        match target {
            Algebraic::Dual(_) => (),
            Algebraic::IndexScan(_) => (),
            Algebraic::Scan(_) => (),
            Algebraic::Project(ref mut p) => self.visit(&mut *p.input),
            Algebraic::Filter(ref mut f) => self.visit(&mut *f.input),
            Algebraic::Join(j) => {
                self.visit(&mut *j.left);
                self.visit(&mut *j.right);
            }
            Algebraic::Union(u) => {
                u.inputs.iter_mut().for_each(|i| self.visit(i));
            }
            Algebraic::Aggregate(a) => self.visit(&mut *a.input),
            Algebraic::Variable(_) => (),
            Algebraic::Set(ref mut s) => {
                if self.current_rule.can_apply(&Algebraic::Set(s.clone())) {
                    let applied = self.current_rule.apply(&mut Algebraic::Set(s.clone()));
                    s.set.extend(applied);
                }
            }
            Algebraic::Sort(v) => self.visit(&mut v.input),
        }
    }
}

fn add_set(mut target: Algebraic) -> AlgSet {
    match target {
        Algebraic::Dual(_) => AlgSet::new(target),
        Algebraic::IndexScan(_) => AlgSet::new(target),
        Algebraic::Scan(_) => AlgSet::new(target),
        Algebraic::Project(ref mut p) => {
            p.input = Box::new(Algebraic::Set(add_set((*p.input).clone())));
            AlgSet::new(target)
        }
        Algebraic::Filter(ref mut f) => {
            f.input = Box::new(Algebraic::Set(add_set((*f.input).clone())));
            AlgSet::new(target)
        }
        Algebraic::Join(ref mut j) => {
            j.left = Box::new(Algebraic::Set(add_set((*j.left).clone())));
            j.right = Box::new(Algebraic::Set(add_set((*j.right).clone())));
            AlgSet::new(target)
        }
        Algebraic::Union(ref mut u) => {
            u.inputs = u
                .inputs
                .iter()
                .map(|u| Algebraic::Set(add_set(u.clone())))
                .collect();
            AlgSet::new(target)
        }
        Algebraic::Aggregate(ref mut a) => {
            a.input = Box::new(Algebraic::Set(add_set((*a.input).clone())));
            AlgSet::new(target)
        }
        Algebraic::Variable(_) => AlgSet::new(target),
        Algebraic::Set(mut s) => {
            s.initial = Box::new(Algebraic::Set(add_set((*s.initial).clone())));
            s
        }
        Algebraic::Sort(ref mut s) => {
            s.input = Box::new(Algebraic::Set(add_set((*s.input).clone())));
            AlgSet::new(target)
        }
    }
}

fn remove_set(mut target: Algebraic) -> Algebraic {
    match &mut target {
        Algebraic::Dual(_) => target,
        Algebraic::IndexScan(_) => target,
        Algebraic::Scan(_) => target,
        Algebraic::Project(ref mut p) => {
            p.input = Box::new(remove_set((*p.input).clone()));
            target
        }
        Algebraic::Filter(ref mut f) => {
            f.input = Box::new(remove_set((*f.input).clone()));
            target
        }
        Algebraic::Join(ref mut j) => {
            j.left = Box::new(remove_set((*j.left).clone()));
            j.right = Box::new(remove_set((*j.right).clone()));
            target
        }
        Algebraic::Union(ref mut u) => {
            u.inputs = u.inputs.iter().map(|i| remove_set(i.clone())).collect();
            target
        }
        Algebraic::Aggregate(ref mut a) => {
            a.input = Box::new(remove_set((*a.input).clone()));
            target
        }
        Algebraic::Variable(_) => target,
        Algebraic::Set(s) => {
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
        Algebraic::Sort(ref mut s) => {
            s.input = Box::new(remove_set((*s.input).clone()));
            target
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::algebra::{Algebraic, Operator};
    use crate::optimize::optimizer::RuleBasedOptimizer;
    use crate::optimize::OptimizeStrategy;
    use std::vec;

    #[test]
    fn project_test() {
        let scan = Algebraic::project(
            Operator::index(0, vec![Operator::input()]),
            Algebraic::project(Operator::input(), Algebraic::table("table".to_string())),
        );

        let optimizer = RuleBasedOptimizer::new();
        let optimized = OptimizeStrategy::RuleBased(optimizer).apply(scan);

        match optimized {
            Algebraic::Project(p) => match p.input.as_ref() {
                Algebraic::Scan(_) => {}
                a => panic!("Expected project but got {:?}", a),
            },
            a => panic!("wrong algebra type {:?}", a),
        }
    }

    #[test]
    fn filter_test() {
        let scan = Algebraic::filter(
            Operator::index(0, vec![Operator::input()]),
            Algebraic::filter(Operator::input(), Algebraic::table("table".to_string())),
        );

        let optimizer = RuleBasedOptimizer::new();
        let optimized = OptimizeStrategy::RuleBased(optimizer).apply(scan);

        match optimized {
            Algebraic::Filter(ref p) => match p.input.as_ref() {
                Algebraic::Scan(_) => {}
                a => panic!("Expected filter but got {:?} in {:?}", a, optimized),
            },
            a => panic!("wrong algebra type {:?}", a),
        }
    }
}
