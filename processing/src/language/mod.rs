use crate::algebra::Algebra::P;
use crate::algebra::{Algebra, Project};
use crate::expression::Expression;
use sqlparser::ast::{Select, SetExpr, Statement, TableFactor};
use sqlparser::dialect::Dialect;
use std::collections::HashMap;

#[derive(Debug)]
pub struct StreamDialect {}

impl Dialect for StreamDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' || ch == '#' || ch == '@' || ch == '$'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic()
            || ch.is_ascii_digit()
            || ch == '@'
            || ch == '$'
            || ch == '#'
            || ch == '_'
    }
}

pub trait Sql {
    fn sql(&self) -> String;
}

pub trait Mql {
    fn mql(&self) -> String;
}

pub trait Cypher {
    fn cypher(&self) -> String;
}

pub fn parse_alg(statements: Vec<Statement>) -> Algebra {
    for statement in statements {
        match statement {
            Statement::Query(q) => {
                if let SetExpr::Select(s) = *q.body {
                    let mut expressions = HashMap::new();

                    for (k, item) in s.projection.iter().enumerate() {
                        expressions.insert(format!("field{}", k), Expression::from(item));
                    }

                    let scan = handle_scan(&s);

                    return P(Project {
                        expressions,
                        input: Box::new(scan),
                    });
                }
            }
            _ => {}
        }
    }
    panic!("No answer")
}

fn handle_scan(s: &Box<Select>) -> Algebra {
    if s.from.len() == 1 {
        if let TableFactor::Table { name, .. } = &s.from[0].relation {
            return Algebra::Scan {
                source: name.to_string(),
            };
        }
        todo!()
    } else {
        for table in &s.from {
            todo!()
        }
        todo!()
    };
}
