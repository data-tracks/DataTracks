use std::str::FromStr;
use std::vec;

use crate::algebra::Operator;
use crate::language::sql::buffer::BufferedLexer;
use crate::language::sql::lex::Token::{As, Comma, From, GroupBy, Identifier, Select, Semi, Star, Text, Where};
use crate::language::sql::statement::{SqlAlias, SqlIdentifier, SqlList, SqlOperator, SqlSelect, SqlStatement, SqlSymbol, SqlValue};
use crate::{algebra, value};
use logos::{Lexer, Logos};

#[derive(Logos, Debug, PartialEq, Clone)]
#[logos(skip r"[ \t\n\f]+")] // Ignore this regex pattern between tokens
pub(crate) enum Token {
    #[regex(r"[a-zA-Z_$][a-zA-Z_$0-9]*\(", | lex | lex.slice().to_owned())]
    Function(String),
    #[regex(r"[']?[a-zA-Z_$][a-zA-Z_$0-9]*[']?", | lex | lex.slice().to_owned())]
    Identifier(String),
    #[regex(r#""[^"\\]*""#, | lex | trim_quotes(lex.slice()))]
    Text(String),
    #[token("false", | _ | false)]
    #[token("true", | _ | true)]
    Bool(bool),
    #[regex(r"-?(?:0|[1-9]\d*)?", | lex | lex.slice().parse::<i64> ().unwrap())]
    Number(i64),
    #[regex(
        r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?", | lex | lex.slice().parse::<f64> ().unwrap()
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
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("/")]
    Divide,
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
    let fields = parse_expressions(lexer, &[From])?;
    let froms = parse_expressions(lexer, &[Semi, Where, GroupBy])?;

    Ok(SqlStatement::Select(SqlSelect::new(fields, froms)))
}

fn parse_expressions(lexer: &mut BufferedLexer, stops: &[Token]) -> Result<Vec<SqlStatement>, String> {
    let mut expressions = vec![];
    let mut stops = stops.to_owned();
    stops.push(Comma);
    let expression = parse_expression(lexer, &stops);
    expressions.push(expression);

    let tok = lexer.consume_buffer();
    if let Ok(t) = tok { // ok to be empty, if no more tokens
        if t == Comma {
            expressions.append(&mut parse_expressions(lexer, &stops)?)
        }
    }
    Ok(expressions)
}

fn parse_expression(lexer: &mut BufferedLexer, stops: &Vec<Token>) -> SqlStatement {
    let mut expressions = vec![];
    let mut operators = vec![];
    let mut operator = None;
    let mut is_alias = false;
    let mut delay = false;

    while let Ok(tok) = lexer.next() {
        if stops.contains(&tok) {
            lexer.buffer(tok);
            break;
        }

        if tok == As {
            is_alias = true;
            break;
        }

        match tok {
            Identifier(i) => {
                expressions.push(SqlStatement::Identifier(SqlIdentifier::new(vec![i])))
            }
            Text(t) => {
                expressions.push(SqlStatement::Value(SqlValue::new(value::Value::text(&t))))
            }
            Token::Number(number) => {
                expressions.push(SqlStatement::Value(SqlValue::new(value::Value::int(number))))
            }
            Token::Float(float) => {
                expressions.push(SqlStatement::Value(SqlValue::new(value::Value::float(float))))
            }
            Token::Function(func) => {
                let stops = vec![Token::BracketClose];
                if let Ok(op) = Operator::from_str(&func){
                    let exp = parse_expressions(lexer,&stops);
                    if let Ok(exprs) = exp {
                        expressions.push(SqlStatement::Operator(SqlOperator::new(op, exprs, true)))
                    }else {
                        panic!("Unknown function arguments!")
                    }
                }else {
                    panic!("Unknown call operator!")
                }
            }

            t => {
                if let Some(op) = parse_operator(t.clone()) {
                    operator = Some(op);
                    if let Some(exp) = expressions.pop() {
                        operators.push(exp);
                    }
                    delay = true;
                } else {
                    panic!("Invalid Token {:?}", t);
                }
            }
        }

        if (delay) {
            delay = false;
        } else if let Some(op) = operator.take() {
            operators.push(expressions.pop().unwrap());
            expressions.push(SqlStatement::Operator(SqlOperator::new(op, operators.drain(..).collect(), false)));
        }
    }

    if let Some(op) = operator.take() {
        if match op {
            Operator::Multiplication(_) => true,
            _ => false
        } {
            return SqlStatement::Symbol(SqlSymbol::new("*"));
        }
    }

    let statement = match expressions.len() {
        1 => {
            expressions.pop().unwrap()
        }
        _ => {
            SqlStatement::List(SqlList::new(expressions))
        }
    };


    if is_alias {
        let alias = parse_expression(lexer, stops);
        return SqlStatement::Alias(SqlAlias::new(statement, alias));
    }
    statement
}

fn parse_operator(tok: Token) -> Option<Operator> {
    match tok {
        Star => Some(Operator::multiplication()),
        Token::Plus => Some(Operator::plus()),
        Token::Minus => Some(Operator::minus()),
        Token::Divide => Some(Operator::divide()),
        _ => None
    }
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
        let query = &select("*", "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_single() {
        let query = &select(&quote("name"), "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_list() {
        let query = &select(&format!("{}, {}", quote("name"), quote("age")), "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_as() {
        let query = &select(&format!("{} AS {}, {}", quote("name"), quote("n"), quote("age")), "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_implicit_join() {
        let query = &select(&format!("{} AS {}, {}", quote("name"), quote("n"), quote("age")), "$0, $1");
        test_query_diff(query, query);
    }

    #[test]
    fn test_as_quote() {
        let query = &select(&format!("{} AS {}, {}", quote("name"), quote("n"), quote("age")), "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add() {
        let query = &select(&format!("{} + 1, {}", quote("name"), quote("age")), "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add_no_space() {
        let query = &select(&format!("{}+1, {}", quote("name"), quote("age")), "$0");
        let res = &select(&format!("{} + 1, {}", quote("name"), quote("age")), "$0");
        test_query_diff(query, res);
    }

    #[test]
    fn test_calculators_sub() {
        let query = &select(&format!("{} - 1, {}", quote("name"), quote("age")), "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_multi() {
        let query = &select(&format!("{} * 1, {}", quote("name"), quote("age")), "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_div() {
        let query = &select(&format!("{} / 1, {}", quote("name"), quote("age")), "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add_nested() {
        let query = &select(&format!("{} + 1 + 1, {}", quote("name"), quote("age")), "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add_nested_mixed() {
        let query = &select(&format!("{} / 1 + 3, {}", quote("name"), quote("age")), "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_function_call() {
        let query = &select(&format!("ADD({}, {})", quote("name"), quote("age")), "$0");
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_function_call_nested() {
        let query = &select(&format!("ADD({}, ADD({}, {}))", quote("name"), quote("age"), quote("age2")), "$0");
        test_query_diff(query, query);
    }

    fn select<'a>(selects: &str, from: &str) -> String {
        format!("SELECT {} FROM {}", selects, from)
    }

    fn quote(key: &str) -> String {
        format!("'{}'", key)
    }

    fn test_query(query: &str) {
        test_query_diff(query, query)
    }

    fn test_query_diff(query: &str, expected: &str) {
        let result = parse(query);
        assert!(matches!(result, Ok(_)), "Expected Ok, but got {:?}", result.err().unwrap());
        let parsed = result.ok().unwrap();
        assert_eq!(parsed.dump("\""), expected, "Expected {:?}, but got {:?}", expected, parsed.dump("\""))
    }
}