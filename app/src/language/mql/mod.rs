use crate::algebra::Algebraic;

mod lex;
mod parse;
mod statement;
mod translate;

pub(crate) fn transform(_query: &str) -> Result<Algebraic, String> {
    todo!()
}
