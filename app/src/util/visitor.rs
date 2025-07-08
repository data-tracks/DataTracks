use crate::algebra::AlgebraRoot;

pub trait CreatingVisitor<Target, Output> {
    fn visit(&self, target: Target) -> Output;
}

pub trait ChangingVisitor<Target> {
    fn visit(&mut self, target: usize, root: &mut AlgebraRoot);
}
