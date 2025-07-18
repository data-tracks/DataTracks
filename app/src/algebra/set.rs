use crate::algebra::AlgebraRoot;
use crate::optimize::{Cost, Rule};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AlgSet {
    pub clone_id: usize,
    pub rules: Vec<Rule>,
    pub alternatives: Vec<usize>,
}

impl AlgSet {
    pub fn new(clone_id: usize) -> AlgSet {
        AlgSet {
            clone_id,
            rules: vec![],
            alternatives: vec![clone_id],
        }
    }

    pub(crate) fn get_cheapest(&self, root: &AlgebraRoot) -> Option<(usize, Cost)> {
        let mut costs = Cost::Infinite;
        let mut alg = None;
        for id in self.alternatives.clone() {
            let current = root.get_node(id)?.calc_cost(root);
            if current < costs {
                costs = current;
                alg = Some(id);
            }
        }
        if let Some(id) = alg {
            Some((id, costs))
        } else {
            None
        }
    }
}
