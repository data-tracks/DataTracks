use crate::algebra::Algebra::{P, S};
use crate::algebra::{Algebra, Project, Scan};
use crate::expression::Expression;
use sqlparser::ast::{Select, SetExpr, Statement, TableFactor};
use sqlparser::dialect::Dialect;

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
                    let mut expressions = vec![];

                    for item in &s.projection {
                        expressions.push(Expression::from(item));
                    }

                    let scan = S(handle_scan(&s));

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

fn handle_scan(s: &Box<Select>) -> Scan {
    if s.from.len() == 1 {
        if let TableFactor::Table { name, .. } = &s.from[0].relation {
            return Scan {
                resource: name.to_string(),
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
