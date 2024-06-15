pub mod sql {
    use crate::language::sql::lex::parse;
    use crate::language::statement::Statement;

    pub fn transform(query: &str) -> Result<Box<dyn Statement>, String> {
        parse(query)
    }
}
