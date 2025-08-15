use crate::language::mql::statement::{MqlDynamic, MqlInsert, MqlStatement, MqlValue};
use logos::Logos;
use serde_json::{Value};
use tracing::debug;

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

pub fn parse<S: AsRef<str>>(query: S) -> Result<MqlStatement, String> {
    let query = query.as_ref().to_string();
    parse_initial(query)
}

fn parse_initial(query: String) -> Result<MqlStatement, String> {
    let query = query.trim();

    // db.create or db.name.find
    let (_, query) = query
        .split_once(".")
        .ok_or("malformed query db[.]coll".to_string())?;

    // name. find
    let (name, action) = query
        .split_once(".")
        .ok_or("malformed query name[.]find(".to_string())?;

    let (action, payload) = action
        .split_once("(")
        .ok_or("malformed query ".to_string())?;

    let payload = payload.strip_suffix(")").ok_or("malformed query end")?;

    // name.find
    match action.to_lowercase().as_str() {
        "insert" => parse_insert(name.to_string(), payload),
        "insertmany" => parse_many_insert(name.to_string(), payload),
        _ => todo!(),
    }
}

fn parse_many_insert(collection: String, docs: &str) -> Result<MqlStatement, String> {
    let value: Value = serde_json::from_str(&format!(r#"{{"key": {docs}}}"#))
        .map_err(|_| "malformed insert many")?;
    todo!()
}

fn parse_insert(collection: String, doc: &str) -> Result<MqlStatement, String> {
    let value = if doc.trim().starts_with("$") {
        handle_dynamic(doc)?
    } else {
        debug!("doc {}", doc);
        let value: value::Value = serde_json::from_str::<Value>(doc)
            .map_err(|_err| "malformed insert")?
            .into();
        MqlStatement::Value(MqlValue { value })
    };

    Ok(MqlStatement::Insert(MqlInsert {
        collection,
        values: Box::new(value),
    }))
}

fn handle_dynamic(val: &str) -> Result<MqlStatement, String> {
    let id = val.strip_prefix("$").ok_or("malformed dynamic value")?.to_string();
    Ok(MqlStatement::Dynamic(MqlDynamic {
        id,
    }))
}

#[cfg(test)]
pub mod tests {
    use crate::language::mql::parse;
    use tracing_test::traced_test;

    #[test]
    #[traced_test]
    fn parse_insert() {
        let query = r#"db.coll.insert({"name":"Peter"})"#;

        let statement = parse(query).unwrap();
        let raw = statement.dump("");
        assert_eq!(raw, query);
    }

    #[test]
    fn parse_insert_dynamic() {
        let query = r#"db.coll.insert($0)"#;

        let statement = parse(query).unwrap();
        let raw = statement.dump("");
        assert_eq!(raw, query);
    }
}
