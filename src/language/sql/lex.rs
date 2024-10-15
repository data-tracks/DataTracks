use std::str::FromStr;
use std::{mem, vec};

use crate::algebra::Op::Tuple;
use crate::algebra::{Op, TupleOp};
use crate::language::sql::buffer::BufferedLexer;
use crate::language::sql::lex::Token::{As, Comma, From, GroupBy, Identifier, Limit, OrderBy, Select, Semi, Star, Text, Where};
use crate::language::sql::statement::{SqlAlias, SqlIdentifier, SqlList, SqlOperator, SqlSelect, SqlStatement, SqlSymbol, SqlValue, SqlVariable};
use crate::value;
use logos::{Lexer, Logos};

#[derive(Logos, Debug, PartialEq, Clone)]
#[logos(skip r"[ \t\n\f]+")] // Ignore this regex pattern between tokens
pub(crate) enum Token {
    #[regex(r"[a-zA-Z_$][a-zA-Z_$0-9]*\(", | lex | lex.slice().to_owned())]
    Function(String),
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
    #[regex("SELECT", ignore(case))]
    Select,
    #[regex("INSERT", ignore(case))]
    Insert,
    #[regex("FROM", ignore(case))]
    From,
    #[regex("WHERE", ignore(case))]
    Where,
    #[regex("GROUP BY", ignore(case))]
    GroupBy,
    #[regex("AS", ignore(case))]
    As,
    #[regex("LIMIT", ignore(case))]
    Limit,
    #[regex("ORDER BY", ignore(case))]
    OrderBy,
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
    #[token("NOT", ignore(case))]
    Not,
    #[token("AND", ignore(case))]
    And,
    #[token("OR", ignore(case))]
    Or,
    #[token("!=")]
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
    #[token("COUNT")]
    Count,
    #[regex(r"[']?[a-zA-Z_$][a-zA-Z_$0-9.]*[']?", | lex | lex.slice().to_owned())]
    Identifier(String),
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
    let froms = parse_expressions(lexer, &[Semi, Where, GroupBy, Limit, OrderBy])?;

    let mut last_end = lexer.consume_buffer();
    let mut wheres = vec![];
    if last_end == Ok(Where) {
        wheres = parse_expressions(lexer, &[Semi, GroupBy, Limit, OrderBy])?;

        last_end = lexer.consume_buffer();
    }

    let mut groups = vec![];
    if last_end == Ok(GroupBy) {
        groups = parse_expressions(lexer, &[Semi, Limit, OrderBy])?
    }


    Ok(SqlStatement::Select(SqlSelect::new(fields, froms, wheres, groups)))
}

fn parse_expressions(lexer: &mut BufferedLexer, stops: &[Token]) -> Result<Vec<SqlStatement>, String> {
    let mut expressions = vec![];
    let mut stops = stops.to_owned();
    stops.push(Comma);
    let expression = parse_expression(lexer, &stops)?;
    expressions.push(expression);

    let tok = lexer.consume_buffer();
    if let Ok(t) = tok { // ok to be empty, if no more tokens
        if t == Comma {
            expressions.append(&mut parse_expressions(lexer, &stops)?)
        } else {
            lexer.buffer(t) // re-add buffer so we can test for stop
        }
    }
    Ok(expressions)
}

fn parse_expression(lexer: &mut BufferedLexer, stops: &Vec<Token>) -> Result<SqlStatement,String> {
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
                expressions.push(SqlStatement::Identifier(SqlIdentifier::new(i.split('.').map(|s| s.to_string()).collect::<Vec<String>>())))
            }
            t if t == Star && expressions.is_empty() => {
                expressions.push(SqlStatement::Symbol(SqlSymbol::new("*")))
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
                if let Ok(op) = Op::from_str(&func) {
                    let exp = parse_expressions(lexer, &stops);
                    if let Ok(exprs) = exp {
                        expressions.push(SqlStatement::Operator(SqlOperator::new(op, exprs, true)))
                    } else {
                        return Err("Unknown function arguments!".to_string())
                    }
                }else if func.starts_with('$') {
                    // variable call
                    let exps = parse_expressions(lexer, &stops)?;

                    let name = func.trim_start_matches('$').to_owned();
                    expressions.push(SqlStatement::Variable(SqlVariable::new(name.trim_end_matches('(').to_string(), exps)))
                }else {
                    return Err("Unknown call operator!".to_string())
                }
            }

            t => {
                if let Some(op) = parse_operator(t.clone()) {
                    operator = Some(op);
                    if let Some(exp) = expressions.pop() {
                        operators.push(exp);
                    }
                    delay = true;
                } else if t == Token::Dot {
                    // nothing on purpose
                } else {
                    return Err( format!("Invalid Token {:?}", t));
                }
            }
        }

        if delay {
            delay = false;
        } else if let Some(op) = operator.take() {
            operators.push(expressions.pop().unwrap());
            expressions.push(SqlStatement::Operator(SqlOperator::new(op, mem::take(&mut operators), false)));
        }
    }

    if let Some(Tuple(TupleOp::Multiplication)) = operator.take() {
        return Ok(SqlStatement::Symbol(SqlSymbol::new("*")));
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
        let alias = parse_expression(lexer, stops)?;
        return Ok(SqlStatement::Alias(SqlAlias::new(statement, alias)));
    }
    Ok(statement)
}

fn parse_operator(tok: Token) -> Option<Op> {
    match tok {
        Star => Some(Op::multiply()),
        Token::Plus => Some(Op::plus()),
        Token::Minus => Some(Op::minus()),
        Token::Divide => Some(Op::divide()),
        Token::Eq => Some(Op::equal()),
        Token::Not => Some(Op::not()),
        Token::And => Some(Op::and()),
        Token::Or => Some(Op::or()),
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
        let query = &select("*", "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_single() {
        let query = &select(&quote("name"), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_list() {
        let query = &select(&format!("{}, {}", quote("name"), quote("age")), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_as() {
        let query = &select(&format!("{} AS {}, {}", quote("name"), quote("n"), quote("age")), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_implicit_join() {
        let query = &select(&format!("{} AS {}, {}", quote("name"), quote("n"), quote("age")), "$0, $1", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_as_quote() {
        let query = &select(&format!("{} AS {}, {}", quote("name"), quote("n"), quote("age")), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add() {
        let query = &select(&format!("{} + 1, {}", quote("name"), quote("age")), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add_no_space() {
        let query = &select(&format!("{}+1, {}", quote("name"), quote("age")), "$0", None, None);
        let res = &select(&format!("{} + 1, {}", quote("name"), quote("age")), "$0", None, None);
        test_query_diff(query, res);
    }

    #[test]
    fn test_calculators_sub() {
        let query = &select(&format!("{} - 1, {}", quote("name"), quote("age")), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_multi() {
        let query = &select(&format!("{} * 1, {}", quote("name"), quote("age")), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_div() {
        let query = &select(&format!("{} / 1, {}", quote("name"), quote("age")), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add_nested() {
        let query = &select(&format!("{} + 1 + 1, {}", quote("name"), quote("age")), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add_nested_mixed() {
        let query = &select(&format!("{} / 1 + 3, {}", quote("name"), quote("age")), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_function_call() {
        let query = &select(&format!("ADD({}, {})", quote("name"), quote("age")), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_function_call_nested() {
        let query = &select(&format!("ADD({}, ADD({}, {}))", quote("name"), quote("age"), quote("age2")), "$0", None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_filter() {
        let query = &select(&format!("{}", quote("name")), "$0", Some(&format!("{} = 3", quote("$0"))), None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_and_filter() {
        let query = &select(&format!("{}", quote("name")), "$0", Some(&format!("{} = 3 and {} = 'test'", quote("$0"), quote("name"))), None);
        let res = &select(&format!("{}", quote("name")), "$0", Some(&format!("{} = 3 AND {} = 'test'", quote("$0"), quote("name"))), None);
        test_query_diff(query, res);
    }

    #[test]
    fn test_or_filter() {
        let query = &select(&format!("{}", quote("name")), "$0", Some(&format!("{} = 3 OR {} = 'test'", quote("$0"), quote("name"))), None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_aggregate_count_single() {
        let query = &select(&format!("COUNT({})", quote("name")), "$0", None, None);
        test_query_diff(query, query);
    }

    fn select(selects: &str, from: &str, wheres: Option<&str>, group_by: Option<&str>) -> String {
        let mut select = format!("SELECT {} FROM {}", selects, from);
        if let Some(wheres) = wheres {
            select += &format!(" WHERE {}", wheres);
        }
        if let Some(group) = group_by {
            select += &format!(" GROUP BY {}", group);
        }
        select
    }

    fn quote(key: &str) -> String {
        format!("'{}'", key)
    }

    fn test_query_diff(query: &str, expected: &str) {
        let result = parse(query);
        assert!(matches!(result, Ok(_)), "Expected Ok, but got {:?}", result.err().unwrap());
        let parsed = result.ok().unwrap();
        assert_eq!(parsed.dump("\""), expected, "Expected {:?}, but got {:?}", expected, parsed.dump("\""))
    }
}