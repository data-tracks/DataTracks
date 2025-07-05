use crate::algebra::AlgebraRoot;

mod lex;
mod parse;
mod statement;
mod translate;

pub(crate) fn transform(_query: &str) -> Result<AlgebraRoot, String> {
    todo!()
}
