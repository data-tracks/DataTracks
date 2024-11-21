use tracing::{debug, warn};
use crate::algebra::{AlgSet, AlgebraType};
use crate::optimize::rule::Rule::Merge;
use crate::optimize::rule::{Rule, RuleBehavior};
use crate::optimize::rules::MergeRule;
use crate::util::{LoadedVisitor, Visitor};

pub enum OptimizeStrategy {
    RuleBased(RuleBasedOptimizer)
}

impl OptimizeStrategy {
    pub(crate) fn apply(&self, raw: AlgebraType) -> AlgebraType {
        let expandable = AlgebraType::Set(SetInserter.visit(raw));
        match self {
            OptimizeStrategy::RuleBased(o) => o.optimize(expandable)
        }
    }

    pub(crate) fn rule_based() -> Self {
        OptimizeStrategy::RuleBased(RuleBasedOptimizer::new())
    }
}

pub trait Optimizer {

    fn optimize(&self, raw: AlgebraType) -> AlgebraType;
}

pub struct RuleBasedOptimizer {
    rules: Vec<Rule>,
}

impl RuleBasedOptimizer {
    pub fn new() -> Self {
        let mut rules:Vec<Rule> = Vec::new();

        rules.push(Merge(MergeRule::Filter));
        rules.push(Merge(MergeRule::Project));
        
        RuleBasedOptimizer { rules }
    }
}


impl Optimizer for RuleBasedOptimizer {
    fn optimize(&self, raw: AlgebraType) -> AlgebraType {
        let mut rules = &self.rules.clone();
        let mut alg = raw.clone();
        let mut round = 0;
        let mut uneventful_rounds = 0;

        while uneventful_rounds < 2 {
            let initial_reward = alg.calc_cost();

            for rule in rules {
                self.visit(&mut alg, rule)
            }

            if initial_reward >= alg.calc_cost() {
                uneventful_rounds += 1;
            }else {
                uneventful_rounds = 0;
            }

            round += 1;
            warn!("round {}", round);
        }
        debug!("Used {} rounds for optimization.", round);
        alg
    }
}


impl LoadedVisitor<&mut AlgebraType, &Rule> for RuleBasedOptimizer {
    fn visit(&self, target: &mut AlgebraType, rule: &Rule) {
        match target {
            AlgebraType::Dual(_) => (),
            AlgebraType::IndexScan(_) => (),
            AlgebraType::TableScan(_) => (),
            AlgebraType::Project(ref mut p) => self.visit(&mut *p.input, rule ),
            AlgebraType::Filter(ref mut f) => self.visit(&mut *f.input, rule ),
            AlgebraType::Join(j) => {
                self.visit(&mut *j.left, rule );
                self.visit(&mut *j.right, rule );
            }
            AlgebraType::Union(u) => {
                u.inputs.iter_mut().for_each(|i| self.visit(i, rule ));
            }
            AlgebraType::Aggregate(a) => self.visit(&mut *a.input, rule ),
            AlgebraType::Variable(_) => (),
            AlgebraType::Set(ref mut s) => {
                let mut algs = vec![];

                s.set.iter_mut().for_each(|a| {
                    self.visit(a, rule);
                    if rule.can_apply(&a) {
                        algs.push(rule.apply(a));
                    }
                });
                s.set.append(&mut algs);

                s.set.iter_mut().for_each(|a| self.visit(a, rule ));
            }
        }
    }
}


pub struct SetInserter;

impl Visitor<AlgebraType, AlgSet> for SetInserter {
    fn visit(&self, target: AlgebraType) -> AlgSet {
        match target {
            AlgebraType::Dual(ref d) => AlgSet::new(target),
            AlgebraType::IndexScan(ref i) => AlgSet::new(target),
            AlgebraType::TableScan(ref s) => AlgSet::new(target),
            AlgebraType::Project(mut p) => {
                p.input = Box::new(AlgebraType::Set(self.visit(*p.input)));
                AlgSet::new(AlgebraType::Project(p))
            }
            AlgebraType::Filter(mut f) => {
                f.input = Box::new(AlgebraType::Set(self.visit(*f.input)));
                AlgSet::new(AlgebraType::Filter(f))
            }
            AlgebraType::Join(mut j) => {
                j.left = Box::new(AlgebraType::Set(self.visit(*j.left)));
                j.right = Box::new(AlgebraType::Set(self.visit(*j.right)));
                AlgSet::new(AlgebraType::Join(j))
            }
            AlgebraType::Union(mut u) => {
                u.inputs = u.inputs.into_iter().map(|u| AlgebraType::Set(self.visit(u))).collect();
                AlgSet::new(AlgebraType::Union(u))
            }
            AlgebraType::Aggregate(mut a) => {
                a.input = Box::new(AlgebraType::Set(self.visit(*a.input)));
                AlgSet::new(AlgebraType::Aggregate(a))
            }
            AlgebraType::Variable(v) => {
                AlgSet::new(AlgebraType::Variable(v))
            }
            AlgebraType::Set(mut s) => {
                s.initial = Box::new(AlgebraType::Set(self.visit(*s.initial)));
                s
            }
        }
    }
}

