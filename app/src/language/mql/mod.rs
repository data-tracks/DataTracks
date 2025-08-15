use crate::algebra::AlgebraRoot;

mod lex;
mod parse;
mod statement;
mod translate;

pub use statement::MqlStatement;

pub use lex::parse;

pub(crate) fn transform(query: &str) -> Result<AlgebraRoot, String> {
    let parse = lex::parse(query)?;
    translate::translate(parse)
}
