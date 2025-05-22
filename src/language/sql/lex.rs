use std::str::FromStr;
use std::{mem, vec};

use crate::algebra::Op::Tuple;
use crate::algebra::{Op, TupleOp};
use crate::language::sql::buffer::BufferedLexer;
use crate::language::sql::lex::Token::{As, BracketClose, Comma, From, GroupBy, Identifier, Limit, OrderBy, Select, Semi, Star, Text, Time, Where, Window};
use crate::language::sql::statement::{SqlAlias, SqlIdentifier, SqlList, SqlOperator, SqlSelect, SqlStatement, SqlSymbol, SqlType, SqlValue, SqlVariable, SqlWindow};
use crate::value;
use crate::value::ValType;
use logos::{Lexer, Logos};
use crate::util::{TimeUnit, WindowType};
use crate::util::TimeUnit::*;

#[derive(Logos, Debug, PartialEq, Clone)]
#[logos(skip r"[ \t\n\f]+")] // Ignore this regex pattern between tokens
pub(crate) enum Token {
    #[regex(r"[a-zA-Z_$][a-zA-Z_$0-9]*\(", | lex | lex.slice().to_owned())]
    Function(String),
    #[regex(r"(?i)text|number", | lex | lex.slice().to_lowercase().to_owned())]
    Type(String),
    #[regex(r#"'[^']*'"#, | lex | trim_quotes(lex.slice()))]
    Text(String),
    #[token("false", | _ | false)]
    #[token("true", | _ | true)]
    Bool(bool),
    #[regex(r"-?(0|[1-9]\d*)", | lex | lex.slice().parse::<i64> ().unwrap(), priority = 3)]
    Number(i64),
    #[regex(
        r"-?(0|[1-9]\d*)?(\.\d+)?([eE][+-]?\d+)?(?:f)?", | lex | lex.slice().trim_end_matches('f').parse::<f64> ().unwrap()
    )] // matches 1.0 .1 1f 1.5e10
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
    #[regex("WINDOW", ignore(case))]
    Window,
    #[regex("THUMBLING", ignore(case))]
    Thumbling,
    #[regex("SIZE", ignore(case))]
    Size,
    #[regex(r"(?i)milis|miliseconds|milisecond", | _ | Millis)]
    #[regex(r"(?i)seconds|second", | _ | Seconds)]
    #[regex(r"(?i)minutes|minute|min", | _ | Minutes)]
    #[regex(r"(?i)hours|hour", | _ | Hours)]
    Time(TimeUnit),
    #[token(",")]
    Comma,
    #[token(".")]
    Dot,
    #[token(";")]
    Semi,
    #[token(":")]
    Colon,
    #[token("null", ignore(case))]
    Null,
    #[token("(")]
    BracketOpen,
    #[token(")")]
    BracketClose,
    #[token("{")]
    CuBracketOpen,
    #[token("}")]
    CuBracketClose,
    #[token("[")]
    SqBracketOpen,
    #[token("]")]
    SqBracketClose,
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
    #[regex(r#"["]?[a-zA-Z_$][a-zA-Z_$0-9.]*["]?"#, | lex | lex.slice().trim_matches('"').to_owned()
    )]
    Identifier(String),
}

pub fn parse(query: &str) -> Result<SqlStatement, String> {
    let mut lexer = create_lexer(query);
    parse_query(&mut lexer)
}

fn parse_query<'source>(lexer: &'source mut Lexer<'source, Token>) -> Result<SqlStatement, String> {
    let mut buf = BufferedLexer::new(lexer);

    let tok = buf.next()?;
    match tok {
        Select => parse_select(&mut buf, &[]),
        _ => Err("Statement is not supported.".to_string()),
    }
}

fn parse_select(lexer: &mut BufferedLexer, stops: &[Token]) -> Result<SqlStatement, String> {
    let fields = parse_expressions(lexer, &[From])?;
    if let Ok(token) = lexer.consume_buffer() {
        // remove from
        if token != From {
            lexer.buffer(token);
        }
    } else {
        // is empty e.g. SELECT [literal]
        return Ok(SqlStatement::Select(SqlSelect::new(
            fields,
            vec![],
            None,
            vec![],
            vec![],
        )));
    }

    let froms = parse_expressions(
        lexer,
        &[&[Semi, Where, GroupBy, Limit, OrderBy, Window], stops].concat(),
    )?;

    let mut last_end = lexer.consume_buffer();
    let mut wheres = vec![];
    if last_end == Ok(Where) {
        wheres = parse_expressions(lexer, &[&[Semi, GroupBy, Limit, OrderBy, Window], stops].concat())?;

        last_end = lexer.consume_buffer();
    }
    let mut window = None;
    if last_end == Ok(Window) {
        window = Some(parse_window(lexer, &[&[Semi, GroupBy, Limit, OrderBy], stops].concat())?);

        last_end = lexer.consume_buffer();
    }

    let mut groups = vec![];
    if last_end == Ok(GroupBy) {
        groups = parse_expressions(lexer, &[&[Semi, Limit, OrderBy], stops].concat())?
    }

    Ok(SqlStatement::Select(SqlSelect::new(
        fields, froms, window, wheres, groups,
    )))
}

fn parse_window(lexer: &mut BufferedLexer, _stops: &[Token]) -> Result<SqlWindow, String> {
    let _type = match lexer.next()? {
        Token::Thumbling => WindowType::Thumbling,
        _ => return Err(format!("Unexpected token {:?}", lexer.next()?)),
    };
    lexer.next()?;// bracket open
    lexer.next()?;// SIZE
    let size = match lexer.next()? {
        Token::Number(size) => size as usize,
        _ => return Err("Invalid window size".to_string()),
    };
    let unit = match lexer.next()? {
        Time(time) => time,
        _ => return Err("Invalid window unit".to_string()),
    };// bracket close
    lexer.next()?;
    Ok(SqlWindow::new(_type, size, unit ))
}

fn parse_expressions(
    lexer: &mut BufferedLexer,
    stops: &[Token],
) -> Result<Vec<SqlStatement>, String> {
    let mut expressions = vec![];
    let mut stops = stops.to_owned();
    stops.push(Comma);
    let expression = parse_expression(lexer, &stops)?;
    expressions.push(expression);

    let tok = lexer.consume_buffer();
    if let Ok(t) = tok {
        // ok to be empty, if no more tokens
        if t == Comma {
            expressions.append(&mut parse_expressions(lexer, &stops)?)
        } else {
            lexer.buffer(t) // re-add buffer so we can test for stop
        }
    }
    Ok(expressions)
}

fn parse_expression(lexer: &mut BufferedLexer, stops: &Vec<Token>) -> Result<SqlStatement, String> {
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
            Identifier(i) => expressions.push(SqlStatement::Identifier(SqlIdentifier::new(
                i.split('.').map(|s| s.to_string()).collect::<Vec<_>>(),
            ))),
            t if t == Star && expressions.is_empty() => {
                expressions.push(SqlStatement::Symbol(SqlSymbol::new("*")))
            }
            Token::Type(t) => {
                expressions.push(SqlStatement::Type(SqlType::new(ValType::parse(&t)?)))
            }
            Text(t) => expressions.push(SqlStatement::Value(SqlValue::new(value::Value::text(&t)))),
            Token::Null => {
                expressions.push(SqlStatement::Value(SqlValue::new(value::Value::null())))
            }
            Token::Number(number) => expressions.push(SqlStatement::Value(SqlValue::new(
                value::Value::int(number),
            ))),
            Token::Float(float) => expressions.push(SqlStatement::Value(SqlValue::new(
                value::Value::float(float),
            ))),
            Token::Function(func) => {
                let stops = vec![BracketClose];
                if let Ok(op) = Op::from_str(&func) {
                    let exp = parse_expressions(lexer, &stops);
                    if let Ok(exprs) = exp {
                        expressions.push(SqlStatement::Operator(SqlOperator::new(op, exprs, true)))
                    } else {
                        return Err(format!("Unknown function arguments: {:?}.", exp));
                    }
                } else if func.starts_with('$') {
                    // variable call
                    let exps = parse_expressions(lexer, &stops)?;

                    let name = func.trim_start_matches('$').to_owned();
                    expressions.push(SqlStatement::Variable(SqlVariable::new(
                        name.trim_end_matches('(').to_string(),
                        exps,
                    )))
                } else {
                    return Err("Unknown call operator!".to_string());
                }
                // empty used stops
                let last = lexer.consume_buffer();
                if let Ok(last) = last {
                    if last != BracketClose {
                        lexer.buffer(last);
                    }
                }
            }

            t => {
                if let Some(op) = parse_operator(t.clone()) {
                    operator = Some(op);
                    if let Some(exp) = expressions.pop() {
                        operators.push(exp);
                    }
                    delay = true;
                } else if t == Token::CuBracketOpen {
                    let doc = parse_doc(lexer)?;
                    expressions.push(doc); // full expression
                } else if t == Token::SqBracketOpen {
                    let stops = vec![Token::SqBracketClose];
                    let array = parse_expressions(lexer, &stops)?;
                    lexer.consume_buffer()?;
                    expressions.push(SqlStatement::Operator(SqlOperator::new(
                        Op::combine(),
                        array,
                        false,
                    )));
                } else if t == Token::Dot {
                    // nothing on purpose
                } else if t == Token::BracketOpen {
                    let next = lexer.next();
                    if !next.is_ok_and(|t| t == Token::Select) {
                        return Err("Subquery did not start with SELECT.".to_string());
                    }
                    let subquery = parse_select(lexer, &[BracketClose])?;
                    expressions.push(subquery);
                } else if t == Select {
                    let subquery = parse_select(lexer, stops)?;
                    expressions.push(subquery);
                    break; // this might require changing
                } else {
                    return Err(format!("Invalid token {:?} stops are: {:?}", t, stops));
                }
            }
        }

        if delay {
            delay = false;
        } else if let Some(op) = operator.take() {
            operators.push(expressions.pop().unwrap());
            expressions.push(SqlStatement::Operator(SqlOperator::new(
                op,
                mem::take(&mut operators),
                false,
            )));
        }
    }

    if let Some(Tuple(TupleOp::Multiplication)) = operator.take() {
        return Ok(SqlStatement::Symbol(SqlSymbol::new("*")));
    }

    let statement = match expressions.len() {
        1 => expressions.pop().unwrap(),
        _ => SqlStatement::List(SqlList::new(expressions)),
    };

    if is_alias {
        let alias = parse_expression(lexer, stops)?;
        return Ok(SqlStatement::Alias(SqlAlias::new(statement, alias)));
    }
    Ok(statement)
}

fn parse_doc(lexer: &mut BufferedLexer) -> Result<SqlStatement, String> {
    let mut pairs = vec![];
    let mut stop = lexer.next_buf()?;
    while stop != Token::CuBracketClose {
        if stop == Comma {
            lexer.consume_buffer()?;
        }

        let key = parse_expression(lexer, &vec![Token::Colon])?;
        lexer.consume_buffer()?;
        let value = parse_expression(lexer, &vec![Token::CuBracketClose, Comma])?;

        let name = if let Some(name) = key.as_literal() {
            name.as_text().map(|t| Some(t.0)).unwrap_or_default()
        } else {
            None
        };

        pairs.push(SqlStatement::Operator(SqlOperator::new(
            Tuple(TupleOp::KeyValue(name)),
            vec![key, value],
            false,
        )));
        stop = lexer.next_buf()?;
    }
    lexer.consume_buffer()?;

    Ok(SqlStatement::Operator(SqlOperator::new(
        Tuple(TupleOp::Doc),
        pairs,
        false,
    )))
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
        _ => None,
    }
}

fn create_lexer(query: &str) -> Lexer<Token> {
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
    use crate::language::sql::lex::{create_lexer, parse, Token};

    #[test]
    fn test_literal_number() {
        let query = &"SELECT 1".to_string();
        test_query_diff(query, query);
    }

    #[test]
    fn test_literal_no_from() {
        let query = &format!("SELECT {}", quote_literal("test"));
        test_query_diff(query, query);
    }

    #[test]
    fn test_number() {
        let mut lexer = create_lexer("1");
        let value = lexer.next().unwrap();
        assert_eq!(value.unwrap(), Token::Number(1));
    }

    #[test]
    fn test_float() {
        let mut lexer = create_lexer("1.1");
        let value = lexer.next().unwrap();
        assert_eq!(value.unwrap(), Token::Float(1.1));
    }

    #[test]
    fn test_float_no_zero() {
        let mut lexer = create_lexer(".1");
        let value = lexer.next().unwrap();
        assert_eq!(value.unwrap(), Token::Float(0.1));
    }

    #[test]
    fn test_float_force() {
        let mut lexer = create_lexer("1f");
        let value = lexer.next().unwrap();
        assert_eq!(value.unwrap(), Token::Float(1.0));
    }

    #[test]
    fn test_float_scientific() {
        let mut lexer = create_lexer("1e-10");
        let value = lexer.next().unwrap();
        assert_eq!(value.unwrap(), Token::Float(1e-10));
    }

    #[test]
    fn test_identifier() {
        let mut lexer = create_lexer(r#"test"#);
        let value = lexer.next().unwrap();
        assert_eq!(value.unwrap(), Token::Identifier(String::from("test")));
    }

    #[test]
    fn test_identifier_without_quotes() {
        let mut lexer = create_lexer("test");
        let value = lexer.next().unwrap();
        assert_eq!(value.unwrap(), Token::Identifier(String::from("test")));
    }

    #[test]
    fn test_text() {
        let mut lexer = create_lexer(r#"'test'"#);
        let value = lexer.next().unwrap();
        assert_eq!(value.unwrap(), Token::Text(String::from("test")));
    }

    #[test]
    fn test_star() {
        let query = &select("*", "$0", None, None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_table() {
        let query = &select("name", "table", None, None, None);
        let query_res = &select(&quote_identifier("name"), "table", None, None, None);
        test_query_diff(query, query_res);
    }

    #[test]
    fn test_dict() {
        let query = "SELECT {\"name\":\"name\", \"id\":1} FROM table";
        test_query_diff(query, query);
    }

    #[test]
    fn test_unwind() {
        let query = "SELECT \"unwind\" FROM UNWIND($0)";
        test_query_diff(query, query);
    }

    #[test]
    fn test_split() {
        let query = "SELECT SPLIT(\"$0\", '\\s+') FROM $0";
        test_query_diff(query, query);
    }

    #[test]
    fn test_scalar_null() {
        let query = "SELECT null";
        test_query_diff(query, query);
    }

    #[test]
    fn test_cast() {
        let query = "SELECT CAST(null AS TEXT)";
        test_query_diff(query, query);
    }

    #[test]
    fn test_single() {
        let query = &select(&quote_identifier("name"), "$0", None, None, None);
        test_query_diff(query, query);
    }

    #[test]
    fn test_list() {
        let query = &select(
            &format!("{}, {}", quote_identifier("name"), quote_identifier("age")),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_as() {
        let query = &select(
            &format!(
                "{} AS {}, {}",
                quote_identifier("name"),
                quote_identifier("n"),
                quote_identifier("age")
            ),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_implicit_join() {
        let query = &select(
            &format!(
                "{} AS {}, {}",
                quote_identifier("name"),
                quote_identifier("n"),
                quote_identifier("age")
            ),
            "$0, $1",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_as_quote() {
        let query = &select(
            &format!(
                "{} AS {}, {}",
                quote_identifier("name"),
                quote_identifier("n"),
                quote_identifier("age")
            ),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add() {
        let query = &select(
            &format!(
                "{} + 1, {}",
                quote_identifier("name"),
                quote_identifier("age")
            ),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add_no_space() {
        let query = &select(
            &format!(
                "{} + 1, {}",
                quote_identifier("name"),
                quote_identifier("age")
            ),
            "$0",
            None,
            None,
            None,
        );
        let res = &select(
            &format!(
                "{} + 1, {}",
                quote_identifier("name"),
                quote_identifier("age")
            ),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, res);
    }

    #[test]
    fn test_calculators_sub() {
        let query = &select(
            &format!(
                "{} - 1, {}",
                quote_identifier("name"),
                quote_identifier("age")
            ),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_multi() {
        let query = &select(
            &format!(
                "{} * 1, {}",
                quote_identifier("name"),
                quote_identifier("age")
            ),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_div() {
        let query = &select(
            &format!(
                "{} / 1, {}",
                quote_identifier("name"),
                quote_identifier("age")
            ),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add_nested() {
        let query = &select(
            &format!(
                "{} + 1 + 1, {}",
                quote_identifier("name"),
                quote_identifier("age")
            ),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_add_nested_mixed() {
        let query = &select(
            &format!(
                "{} / 1 + 3, {}",
                quote_identifier("name"),
                quote_identifier("age")
            ),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_function_call() {
        let query = &select(
            &format!(
                "ADD({}, {})",
                quote_identifier("name"),
                quote_identifier("age")
            ),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_calculators_function_call_nested() {
        let query = &select(
            &format!(
                "ADD({}, ADD({}, {}))",
                quote_identifier("name"),
                quote_identifier("age"),
                quote_identifier("age2")
            ),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_filter() {
        let query = &select(
            &format!("{}", quote_identifier("name")),
            "$0",
            Some(&format!("{} = 3", quote_identifier("$0"))),
            None,
            None
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_and_filter() {
        let query = &select(
            &format!("{}", quote_identifier("name")),
            "$0",
            Some(&format!(
                "{} = 3 and {} = 'test'",
                quote_identifier("$0"),
                quote_identifier("name")
            )),
            None,
            None
        );
        let res = &select(
            &format!("{}", quote_identifier("name")),
            "$0",
            Some(&format!(
                "{} = 3 AND {} = 'test'",
                quote_identifier("$0"),
                quote_identifier("name")
            )),
            None,
            None
        );
        test_query_diff(query, res);
    }

    #[test]
    fn test_or_filter() {
        let query = &select(
            &format!("{}", quote_identifier("name")),
            "$0",
            Some(&format!(
                "{} = 3 OR {} = 'test'",
                quote_identifier("$0"),
                quote_identifier("name")
            )),
            None,
            None
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_aggregate_count_single() {
        let query = &select(
            &format!("COUNT({})", quote_identifier("name")),
            "$0",
            None,
            None,
            None,
        );
        test_query_diff(query, query);
    }

    #[test]
    fn test_window() {
        let query = &select("COUNT(*)", "$0", None, None, Some("THUMBLING (SIZE 5 SECONDS)"));
        test_query_diff(query, query);
    }

    fn select(
        selects: &str,
        from: &str,
        wheres: Option<&str>,
        group_by: Option<&str>,
        window: Option<&str>,
    ) -> String {
        let mut select = format!("SELECT {} FROM {}", selects, from);
        if let Some(window) = window {
            select += &format!(" WINDOW {}", window);
        }
        if let Some(wheres) = wheres {
            select += &format!(" WHERE {}", wheres);
        }
        if let Some(group) = group_by {
            select += &format!(" GROUP BY {}", group);
        }
        select
    }

    fn quote_identifier(key: &str) -> String {
        format!("\"{}\"", key)
    }

    fn quote_literal(key: &str) -> String {
        format!("'{}'", key)
    }

    fn test_query_diff(query: &str, expected: &str) {
        let result = parse(query);
        assert!(
            matches!(result, Ok(_)),
            "Expected Ok, but got {:?}",
            result.err().unwrap()
        );
        let parsed = result.ok().unwrap();
        assert_eq!(
            parsed.dump("\""),
            expected,
            "Expected {:?}, but got {:?}",
            expected,
            parsed.dump("\"")
        )
    }
}
