use crate::algebra::AlgebraRoot;
use crate::language::sql::lex::parse;
use crate::language::sql::translate::translate;

pub fn transform(query: &str) -> Result<AlgebraRoot, String> {
    let parse = parse(query)?;
    translate(parse)
}
