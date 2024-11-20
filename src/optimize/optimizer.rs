use crate::algebra::AlgebraType;
use crate::optimize::rule::Rule;
use crate::optimize::rules::MergeRule;

pub enum OptimizeStrategy {
    RuleBased(RuleBasedOptimizer)
}

impl OptimizeStrategy {
    pub(crate) fn apply(&self, raw: AlgebraType) -> AlgebraType {
        match self {
            OptimizeStrategy::RuleBased(o) => o.optimize(raw)
        }
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
        let mut rules = Vec::new();

        rules.push(MergeRule::Filter);
        rules.push(MergeRule::Project);


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
            let initial_effort = alg.calc_effort();

            for rule in rules {
                if rule.can_apply(&alg) {
                    alg = rule.apply(&raw);
                }
            }

            if initial_effort >= alg.calc_effort() {
                uneventful_rounds += 1;
            }

            round += 1;
        }
        alg
    }
}