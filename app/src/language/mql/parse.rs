use crate::algebra::AlgebraType;

pub fn transform(query: &str) -> Result<AlgebraType, String> {
    let parse = crate::language::mql::lex::parse(query)?;
    crate::language::mql::translate::translate(parse)
}
