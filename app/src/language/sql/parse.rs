use crate::algebra::Algebraic;
use crate::language::sql::lex::parse;
use crate::language::sql::translate::translate;

pub fn transform(query: &str) -> Result<Algebraic, String> {
    let parse = parse(query)?;
    translate(parse)
}
