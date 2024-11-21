pub trait Visitor<Target, Output> {
    fn visit(&self, target: Target) -> Output;
}


pub trait LoadedVisitor<Target, Arg> {
    fn visit(&self, target: Target, arg: Arg);
}