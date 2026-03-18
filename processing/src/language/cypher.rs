use crate::expression::Expression;
use crate::{Algebra, Project, Scan, Schema};
use anyhow::anyhow;
use indexmap::IndexMap;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alpha1, alphanumeric1, multispace1};
use nom::combinator::recognize;
use nom::multi::many0;
use nom::sequence::{delimited, pair};
use nom::IResult;
use nom::Parser;

#[derive(Debug, PartialEq)]
pub struct MatchQuery {
    pub src: String,          // e.g., "source"
    pub label: String,        // e.g., "Person"
    pub alias: String,        // e.g., "n"
    pub return_field: String, // e.g., "age"
}

// Parser for identifiers (n, source, Person, age)
fn identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alt((alpha1, tag("_"), tag("$"))),
        many0(alt((alphanumeric1, tag("_"), tag("$")))),
    ))
    .parse(input)
}

// Parses: (n:$$source:Person)
fn parse_pattern(input: &str) -> IResult<&str, (&str, &str, &str)> {
    delimited(
        tag("("),
        (
            identifier, // "n"
            tag(":"),   // ":$$"
            identifier, // "source"
            tag(":"),   // ":"
            identifier, // "Person"
        ),
        tag(")"),
    )
    .parse(input)
    .map(|(next, (alias, _, src, _, label))| (next, (alias, src, label)))
}

impl From<MatchQuery> for Algebra {
    fn from(m: MatchQuery) -> Self {
        let scan = Algebra::Scan(Scan {
            source: m.src,
            schema: Schema::Dynamic,
        });
        Algebra::Project(Project {
            expressions: IndexMap::from([(
                m.return_field.to_string(),
                Expression::field(&m.return_field),
            )]),
            input: Box::new(scan),
        })
    }
}

fn parse_cypher_query(input: &str) -> IResult<&str, MatchQuery> {
    let (input, _) = tag("MATCH")(input)?;
    let (input, _) = multispace1(input)?;

    let (input, (alias, src, label)) = parse_pattern(input)?;

    let (input, _) = multispace1(input)?;
    let (input, _) = tag("RETURN")(input)?;
    let (input, _) = multispace1(input)?;

    // Parses "n.age"
    let (input, _) = tag(alias)(input)?;
    let (input, _) = tag(".")(input)?;
    let (input, return_field) = identifier(input)?;

    Ok((
        input,
        MatchQuery {
            src: src.to_string(),
            label: label.to_string(),
            alias: alias.to_string(),
            return_field: return_field.to_string(),
        },
    ))
}

pub fn parse_cypher(input: &str) -> anyhow::Result<Algebra> {
    let (_, query) = parse_cypher_query(input).map_err(|e| anyhow!(e.to_string()))?;
    Ok(query.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_cypher() {
        let query = "MATCH (n:$$source:Person) RETURN n.age";
        let result = parse_cypher_query(query);

        assert!(result.is_ok());
        let (remaining, parsed) = result.unwrap();

        // Ensure all input was consumed
        assert_eq!(remaining.trim(), "");

        // Verify the struct fields
        assert_eq!(parsed.alias, "n");
        assert_eq!(parsed.src, "$$source");
        assert_eq!(parsed.label, "Person");
        assert_eq!(parsed.return_field, "age");
    }

    #[test]
    fn test_parse_with_extra_whitespace() {
        let query = "MATCH   (n:$$source:Person)   RETURN   n.age";
        let result = parse_cypher_query(query);

        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_mismatched_alias() {
        let query = "MATCH (n:$$source:Person) RETURN m.age";
        let result = parse_cypher_query(query);

        assert!(result.is_err());
    }

    #[test]
    fn test_identifier() {
        let query = "age";
        let (_, id) = identifier(query).unwrap();
        assert_eq!(id, query);
    }

    #[test]
    fn test_parse_cypher_alg() {
        let query = "MATCH (n:$$source:Person) RETURN n.age";
        let result = parse_cypher(query).unwrap();
    }
}
