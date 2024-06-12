use logos::{Lexer, Logos, Source};
use crate::language::sql::buffer::BufferedLexer;
use crate::language::sql::lex::Token::{From, GroupBy, Semi, Where};
use crate::language::sql::statement::{Sql, SqlSelect, Statement};

#[derive(Logos, Debug, PartialEq, Clone)]
#[logos(skip r"[ \t\n\f]+")] // Ignore this regex pattern between tokens
pub(crate) enum Token {
    #[regex(r"[a-zA-Z_$][a-zA-Z_$0-9]*", | lex | trim_quotes(lex.slice()))]
    Identifier(String),
    #[regex(r#"["|'][a-zA-Z]+["|']"#, | lex | lex.slice().to_owned())]
    Text(String),
    #[token("false", | _ | false)]
    #[token("true", | _ | true)]
    Bool(bool),
    #[regex(r"-?(?:0|[1-9]\d*)?", | lex | lex.slice().parse::< i64 > ().unwrap())]
    Number(i64),
    #[regex(
        r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?", | lex | lex.slice().parse::< f64 > ().unwrap()
    )]
    Float(f64),
    #[regex(r"(?i)SELECT")]
    Select,
    #[regex(r"(?i)INSERT")]
    Insert,
    #[regex(r"(?i)FROM")]
    From,
    #[regex(r"(?i)WHERE")]
    Where,
    #[regex(r"(?i)GROUP BY")]
    GroupBy,
    #[regex(r"(?i)AS")]
    As,
    #[token(",")]
    Comma,
    #[token(".")]
    Dot,
    #[token(";")]
    Semi,
    #[token(":")]
    Colon,
    #[token("null")]
    Null,
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

pub fn parse(query: String) -> Result<Box<dyn Statement>, String> {
    let mut lexer = crate_lexer(&query);
    parse_query(&mut lexer)
}

fn parse_query<'source>(lexer: &'source mut Lexer<'source, Token>) -> Result<Box<dyn Statement>, String> {
    let mut buf = BufferedLexer::new(lexer);

    let tok = buf.next()?;
    match tok {
        Token::Select => parse_select(&mut buf),
        Token::Insert => parse_insert(&mut buf),
        _ => Err("Statement is not supported.".to_string())
    }
}

fn parse_insert(lexer: &mut BufferedLexer) -> Result<Box<dyn Statement>, String> {
    todo!()
}

fn parse_select(lexer: &mut BufferedLexer) -> Result<Box<dyn Statement>, String> {
    let columns = vec![];
    while let Ok(tok) = lexer.next() {
        if tok == From {
            break;
        }
    }
    let froms = vec![];
    while let Ok(tok) = lexer.next() {
        if vec![Semi, Where, GroupBy].contains(&tok){
            break;
        }
    }

    Ok(Box::new(SqlSelect::new(columns, froms, )))
}

fn crate_lexer(query: &str) -> Lexer<Token> {
    Token::lexer(query)
}


fn trim_quotes(value: &str) -> String {
    let mut chars = value.chars();
    chars.next();
    chars.next_back();
    chars.as_str().to_string()
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
            "Select \"name\" AS n, age FROM $0",
        ];

        for query in queries {
            let result = parse(query.to_string());
            assert!(matches!(result, Ok(_)), "Expected Ok, but got {}", result.err().unwrap())
        }
    }

    #[test]
    fn parse_dump_test() {
        let queries = vec![
            "SELECT * FROM $0",
            "SELECT name, age FROM $0",
            "SELECT name AS n, age FROM $0",
            "Select 'name' AS n, age FROM $0",
            "Select \"name\" AS n, age FROM $0",
        ];

        for query in queries {
            let result = parse(query.to_string());
        }
    }
}