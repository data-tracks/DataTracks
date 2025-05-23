use logos::Logos;

use crate::language::mql::statement::MqlStatement;

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

fn trim_quotes(value: &str) -> String {
    let mut chars = value.chars();
    chars.next();
    chars.next_back();
    chars.as_str().to_string()
}

pub(crate) fn parse(_query: &str) -> Result<MqlStatement, String> {
    todo!()
}