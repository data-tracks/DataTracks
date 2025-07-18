use crate::algebra::visitor::Visitor;
use crate::algebra::{Algebra, AlgebraRoot, Algebraic};

pub struct Purger {
    visited: Vec<usize>,
}

impl Purger {
    pub fn new() -> Self {
        Purger { visited: vec![] }
    }

    pub fn purge(&mut self, root: &mut AlgebraRoot) {
        root.traverse(self)
    }
}

impl Visitor for Purger {
    fn visit(&mut self, alg: &Algebraic) {
        self.visited.push(alg.id());
    }
}

#[cfg(test)]
mod tests {
    use crate::algebra::AlgebraRoot;
    use crate::algebra::analyse::purge::Purger;

    #[test]
    fn test_basic() {
        let mut purger = Purger::new();

        let mut root = AlgebraRoot::new_scan_index(0);

        purger.purge(&mut root);

        assert_eq!(purger.visited, vec![0]);
    }
}
