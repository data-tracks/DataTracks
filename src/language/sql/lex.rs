use std::vec;

use logos::{Lexer, Logos};

use crate::language::sql::buffer::BufferedLexer;
use crate::language::sql::lex::Token::{As, Comma, From, GroupBy, Identifier, Select, Semi, Star, Text, Where};
use crate::language::sql::statement::{SqlIdentifier, SqlSelect, SqlStatement, SqlSymbol};

#[derive(Logos, Debug, PartialEq, Clone)]
#[logos(skip r"[ \t\n\f]+")] // Ignore this regex pattern between tokens
pub(crate) enum Token {
    #[regex(r"[a-zA-Z_$][a-zA-Z_$0-9]*", | lex | lex.slice().to_owned())]
    Identifier(String),
    #[regex(r#"["|'][a-zA-Z]+["|']"#, | lex | trim_quotes(lex.slice()))]
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

pub fn parse(query: &str) -> Result<SqlStatement, String> {
    let mut lexer = crate_lexer(query);
    parse_query(&mut lexer)
}

fn parse_query<'source>(lexer: &'source mut Lexer<'source, Token>) -> Result<SqlStatement, String> {
    let mut buf = BufferedLexer::new(lexer);

    let tok = buf.next()?;
    match tok {
        Select => parse_select(&mut buf),
        _ => Err("Statement is not supported.".to_string())
    }
}

fn parse_select(lexer: &mut BufferedLexer) -> Result<SqlStatement, String> {
    let fields = parse_expressions(lexer, &vec![From])?;
    let froms = parse_expressions(lexer, &vec![Semi, Where, GroupBy])?;

    Ok(SqlStatement::Select(SqlSelect::new(fields, froms)))
}

fn parse_expressions(lexer: &mut BufferedLexer, stops: &[Token]) -> Result<Vec<SqlStatement>, String> {
    let mut expressions = vec![];
    let mut stops = stops.to_owned();
    stops.push(Comma);
    expressions.push(parse_expression(lexer, &stops));

    let tok = lexer.consume_buffer();
    if let Ok(t) = tok { // ok to be empty, if no more tokens
        if t == Comma {
            expressions.append(&mut parse_expressions(lexer, &stops)?)
        }
    }
    Ok(expressions)
}

fn parse_expression(lexer: &mut BufferedLexer, stops: &Vec<Token>) -> SqlStatement {
    let mut expression = vec![];
    let mut is_alias = false;
    while let Ok(tok) = lexer.next() {
        if stops.contains(&tok) {
            lexer.buffer(tok);
            return SqlStatement::Identifier(SqlIdentifier::new(expression, None));
        }

        if tok == Star {
            return SqlStatement::Symbol(SqlSymbol::new("*"));
        }

        if tok == As {
            is_alias = true;
            break;
        }

        match tok {
            Identifier(i) => {
                expression.push(i)
            }
            Text(t) => {
                expression.push(t)
            }
            _ => {}
        }
    }
    let mut alias = None;
    if is_alias {
        alias = Some(parse_expression(lexer, stops));
    }

    SqlStatement::Identifier(SqlIdentifier::new(expression, alias))
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
    fn test_star() {
        test_query("SELECT * FROM $0");
    }

    #[test]
    fn test_single() {
        test_query("SELECT name FROM $0");
    }

    #[test]
    fn test_list() {
        test_query("SELECT name, age FROM $0");
    }

    #[test]
    fn test_as() {
        test_query("SELECT name AS n, age FROM $0");
    }

    #[test]
    fn test_implicit_join() {
        test_query("SELECT name AS n, age FROM $0, $1");
    }

    #[test]
    fn test_as_mixed() {
        test_query_diff("Select 'name' AS n, age FROM $0", "SELECT name AS n, age FROM $0");
    }

    #[test]
    fn test_as_quote() {
        test_query_diff("Select \"name\" AS n, age FROM $0", "SELECT name AS n, age FROM $0");
    }

    fn test_query(query: &str) {
        test_query_diff(query, query)
    }

    fn test_query_diff(query: &str, expected: &str) {
        let result = parse(query);
        assert!(matches!(result, Ok(_)), "Expected Ok, but got {:?}", result.err().unwrap());
        let parsed = result.ok().unwrap();
        assert_eq!(parsed.dump(), expected, "Expected {:?}, but got {:?}", expected, parsed.dump())
    }
}