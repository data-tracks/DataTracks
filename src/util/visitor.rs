pub trait Visitor<Output> {
    fn visit(&mut self) -> Output;
}