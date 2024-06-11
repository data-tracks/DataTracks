use logos::Logos;

#[derive(Logos, Debug, PartialEq)]
#[logos(skip r"[ \t\n\f]+")] // Ignore this regex pattern between tokens
enum Token {
    #[regex("[a-zA-Z_$][a-zA-Z_$0-9]*")]
    Identifier,
    #[regex(r#""["].*["]""#,)]
    Text,
    #[regex(r"(?i)SELECT")]
    Select,
    #[token(r"(?i)FROM")]
    From,
    #[token(r"(?i)WHERE")]
    Where,
    #[token(r"(?i)AS")]
    As,
    #[token(",")]
    Comma,
    #[token(".")]
    Dot,
    #[token("(")]
    BracketOpen,
    #[token(")")]
    BracketClose,
    #[token("=")]
    Eq,
    #[token("<>")]
    Ne,
    #[token("*")]
    Star,
}

pub fn parse(query: String) -> Result<(), String> {
    let mut lexer = Token::lexer(&query);
    while let Some(token) = lexer.next() {
        println!("{:?}", token);
    }
    println!();
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::language::sql::lex::parse;

    #[test]
    fn default_test() {
        let queries = vec![
            "SELECT * FROM $0",
            "SELECT name, age FROM $0",
            "SELECT name AS n, age FROM $0",
            "Select 'name' AS n, age FROM $0",
        ];

        for query in queries {
            let result = parse(query.to_string());
            assert!(matches!(result, Ok(_)), "Expected Ok, got {:?}", result)
        }
    }
}