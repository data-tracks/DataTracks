use crate::algebra::AlgebraType;

mod lex;
mod parse;
mod statement;
mod translate;

pub(crate) fn transform(_query: &str) -> Result<AlgebraType, String> {
    todo!()
}
