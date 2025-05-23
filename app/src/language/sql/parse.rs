use crate::algebra::AlgebraType;
use crate::language::sql::lex::parse;
use crate::language::sql::translate::translate;

pub fn transform(query: &str) -> Result<AlgebraType, String> {
    let parse = parse(query)?;
    translate(parse)
}

