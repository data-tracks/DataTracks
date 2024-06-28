use crate::algebra::AlgebraType;

mod lex;
mod parse;
mod translate;
mod statement;

pub(crate) fn transform(p0: &str) -> Result<AlgebraType, String> {
    todo!()
}