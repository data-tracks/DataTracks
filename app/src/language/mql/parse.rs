use crate::algebra::AlgebraRoot;

pub fn transform(query: &str) -> Result<AlgebraRoot, String> {
    let parse = crate::language::mql::lex::parse(query)?;
    crate::language::mql::translate::translate(parse)
}
