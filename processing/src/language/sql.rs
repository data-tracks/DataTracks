use crate::expression::Expression;
use crate::{Algebra, Project, Scan, Schema};
use indexmap::IndexMap;
use sqlparser::ast::{Select, SetExpr, Statement, TableFactor};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use tracing::debug;

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

fn parse_alg(statements: Vec<Statement>) -> Algebra {
    for statement in statements {
        if let Statement::Query(q) = statement {
            if let SetExpr::Select(s) = *q.body {
                let mut expressions = IndexMap::new();

                for (k, item) in s.projection.iter().enumerate() {
                    expressions.insert(format!("field{}", k), Expression::from(item));
                }

                let scan = handle_scan(&s);

                return Algebra::Project(Project {
                    expressions,
                    input: Box::new(scan),
                });
            }
        }
    }
    panic!("No answer")
}

pub fn parse_sql(query: &str) -> Algebra {
    let dialect = StreamDialect {};

    let ast = Parser::parse_sql(&dialect, query).unwrap();

    debug!("{:?}", ast);

    parse_alg(ast)
}

fn handle_scan(s: &Select) -> Algebra {
    if s.from.len() == 1 {
        if let TableFactor::Table { name, .. } = &s.from[0].relation {
            return Algebra::Scan(Scan {
                source: name.to_string(),
                schema: Schema::Dynamic,
            });
        }
        todo!()
    } else {
        for _ in &s.from {
            todo!()
        }
        todo!()
    };
}
