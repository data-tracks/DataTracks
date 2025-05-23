pub trait Statement {
    fn dump(&self, quote: &str) -> String;
}