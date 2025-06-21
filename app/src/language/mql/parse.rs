use crate::algebra::Algebraic;

pub fn transform(query: &str) -> Result<Algebraic, String> {
    let parse = crate::language::mql::lex::parse(query)?;
    crate::language::mql::translate::translate(parse)
}
