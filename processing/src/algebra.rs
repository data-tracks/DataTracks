use crate::expression::Expression;
use crate::language::Sql;
use crate::operator::Operator;
use crate::program::Program;
use indexmap::IndexMap;
use serde::Serialize;
use std::cmp;
use value::ValType;

#[derive(Clone, Debug, Serialize)]
pub enum Algebra {
    Scan { source: String, schema: Schema },
    P(Project),
    F(Filter),
    C(Collect),
    U(Unwind),
    T(String),
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Scope {
    Tuple = 0,
    Multi = 1,
    Join = 2,
}

impl Algebra {
    #[cfg(test)]
    pub(crate) fn project<M: Into<IndexMap<String, Expression>>>(
        child: Algebra,
        expressions: M,
    ) -> Self {
        Algebra::P(Project {
            expressions: expressions.into(),
            input: Box::new(child),
        })
    }

    pub(crate) fn filter(child: Algebra, predicate: Expression) -> Self {
        Algebra::F(Filter {
            predicate,
            input: Box::new(child),
        })
    }

    #[cfg(test)]
    pub(crate) fn scan<S: AsRef<str>>(resource: S, schema: Schema) -> Self {
        Algebra::Scan {
            source: resource.as_ref().to_string(),
            schema,
        }
    }

    #[cfg(test)]
    pub(crate) fn unwind<S: AsRef<str>>(child: Algebra, key: S, func: Operator) -> Self {
        Algebra::U(Unwind {
            input: Box::new(child),
            key: key.as_ref().to_string(),
            func,
        })
    }

    pub fn scope(&self) -> Scope {
        match self {
            Algebra::Scan { .. } => Scope::Tuple,
            Algebra::P(p) => cmp::max(
                p.input.scope(),
                p.expressions
                    .iter()
                    .map(|(_, e)| e.scope())
                    .fold(Scope::Tuple, |a, b| cmp::max(a, b)),
            ),
            Algebra::F(f) => cmp::max(f.input.scope(), f.predicate.scope()),
            Algebra::C(_) => Scope::Multi,
            Algebra::U(_) => Scope::Multi,
            Algebra::T(_) => Scope::Tuple,
        }
    }

    pub fn schema(&self) -> Schema {
        Schema::Fixed(IndexMap::from([("price".to_string(), ValType::Float)]))
    }

    pub fn set_schema(&mut self, s: Schema) {
        match self {
            Algebra::Scan { schema, .. } => *schema = s,
            Algebra::P(p) => p.input.set_schema(s),
            Algebra::F(f) => f.input.set_schema(s),
            Algebra::C(c) => c.input.set_schema(s),
            Algebra::U(u) => u.input.set_schema(s),
            Algebra::T(_) => {}
        }
    }

    pub fn processing(&self) -> Program {
        Program::from(self)
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum Schema {
    Dynamic,
    Fixed(IndexMap<String, ValType>),
}

impl Schema {
    pub fn fixed<M: Into<IndexMap<String, ValType>>>(fields: M) -> Self {
        Schema::Fixed(fields.into())
    }

    pub fn get(&self, name: &str) -> Option<usize> {
        match self {
            Schema::Dynamic => Some(0),
            Schema::Fixed(f) => f.get_index_of(name),
        }
    }
}

impl Sql for Algebra {
    fn sql(&self) -> String {
        match self {
            Algebra::Scan { source, .. } => format!("FROM {}", source),
            Algebra::P(p) => p.sql(),
            Algebra::F(f) => f.sql(),
            Algebra::T(_) => panic!(),
            Algebra::C(_) => panic!(),
            Algebra::U(_) => panic!(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Collect {
    input: Box<Algebra>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Unwind {
    pub(crate) input: Box<Algebra>,
    pub(crate) key: String,
    pub(crate) func: Operator,
}

#[derive(Clone, Debug, Serialize)]
pub struct Project {
    pub expressions: IndexMap<String, Expression>,
    pub input: Box<Algebra>,
}

impl Sql for Project {
    fn sql(&self) -> String {
        let select = format!(
            "SELECT {}",
            self.expressions
                .iter()
                .map(|(_, e)| e.sql())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let child = self.input.sql();
        format!("{} {}", select, child)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Filter {
    pub predicate: Expression,
    pub input: Box<Algebra>,
}

impl Sql for Filter {
    fn sql(&self) -> String {
        format!(" WHERE {}", self.predicate.sql())
    }
}

#[cfg(test)]
mod test {
    use crate::language::{Sql, parse_sql};
    use tracing::debug;

    #[test]
    // SELECT Istream(auction, DOLTOEUR(price), bidder, datetime) FROM bid [ROWS UNBOUNDED]
    // simple multiplier
    fn nexmark_q1_sql() {
        let q1_sql = "SELECT auction, price * 1.1, bidder, datetime FROM $$source";

        let algebra = parse_sql(q1_sql);

        let sql = algebra.sql();
        debug!("{:?}", sql);
    }

    #[test]
    // SELECT Istream(auction, DOLTOEUR(price), bidder, datetime) FROM bid [ROWS UNBOUNDED]
    // simple multiplier
    fn nexmark_q1_mongodb() {
        let q1_mql = "db.$source.aggregate([ $$project: {auction: 1, price: {$multiply: [\"$price\", 1.1]}, bidder, datetime}])";

        //let dialect = StreamDialect {};

        //let ast = Parser::parse_sql(&dialect, q1_mql).unwrap();

        //println!("{:?}", ast);
    }

    #[test]
    // SELECT Istream(auction, DOLTOEUR(price), bidder, datetime) FROM bid [ROWS UNBOUNDED]
    // simple multiplier
    fn nexmark_q1_cypher() {
        let q1_cypher = "MATCH (n:$$source) RETURN n.auction, n.price * 1.1, n.bidder, n.datetime";

        //let ast = parse(q1_cypher).ast.unwrap();

        //println!("{:?}", ast);
    }
}
