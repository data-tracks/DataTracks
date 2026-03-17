use crate::expression::Expression;
use crate::{Algebra, Project, Scan, Schema};
use anyhow::anyhow;
use mongodb::bson;
use mongodb::bson::Array;
use nom::bytes::complete::{tag, take, take_until};
use nom::character::complete::char;
use nom::{IResult, Input};
use serde_json::Value;

#[derive(Debug, PartialEq)]
struct MongoCommand {
    collection: String,
    payload: Array,
}

fn parse_db_call(input: &str) -> IResult<&str, MongoCommand, nom::error::Error<&str>> {
    let (input, _) = tag("db.")(input)?;

    let (input, collection) = take_until(".")(input)?;
    let (input, _) = tag(".aggregate(")(input)?;

    let content_len = input.input_len().saturating_sub(1);
    let (input, payload) = take(content_len)(input)?;
    let (input, _) = char(')')(input)?;

    let value: Value = json5::from_str(payload).unwrap();
    let payload = bson::to_bson(&value).unwrap();

    Ok((
        input,
        MongoCommand {
            collection: collection.to_string(),
            payload: payload.as_array().unwrap().clone(),
        },
    ))
}

fn parse_call<S: AsRef<str>>(input: S) -> anyhow::Result<MongoCommand> {
    parse_db_call(input.as_ref())
        .map(|(_, command)| command)
        .map_err(|e| anyhow!(e.to_string()))
}

pub fn parse_mql<S: AsRef<str>>(input: S) -> anyhow::Result<Algebra> {
    Ok(parse_call(input.as_ref())?.into())
}

impl Into<Algebra> for MongoCommand {
    fn into(self) -> Algebra {
        let mut node = Algebra::Scan(Scan {
            source: self.collection.clone(),
            schema: Schema::Dynamic,
        });
        for value in self.payload {
            let (key, value) = value.as_document().unwrap().into_iter().next().unwrap();

            if key == "$project" {
                let expressions = value
                    .as_document()
                    .unwrap()
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), Expression::from((k.as_str(), v))))
                    .collect();

                node = Algebra::Project(Project {
                    expressions,
                    input: Box::new(node),
                });
            }
        }
        node
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_db_call() {
        let input = "db.$$source.aggregate([{$project: {}}])";
        if let Ok(command) = parse_call(input) {
            assert_eq!(command.collection, "$$source");
            println!("payload:  {:?}", command.payload);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_parse_db_call_alg() {
        let input = "db.$$source.aggregate([{$project: {name: 1}}])";
        if let Ok(alg) = parse_mql(input) {
            let alg: Algebra = alg;
            println!("{:?}", alg)
        } else {
            assert!(false);
        }
    }
}
