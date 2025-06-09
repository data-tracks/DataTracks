pub trait CreatingVisitor<Target, Output> {
    fn visit(&self, target: Target) -> Output;
}

pub trait ChangingVisitor<Target> {
    fn visit(&self, target: Target);
}
