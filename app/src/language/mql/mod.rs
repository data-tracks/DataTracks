use crate::algebra::AlgebraType;

mod lex;
mod parse;
mod translate;
mod statement;

pub(crate) fn transform(_query: &str) -> Result<AlgebraType, String> {
    todo!()
}