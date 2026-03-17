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
    Scan(Scan),
    Project(Project),
    Filter(Filter),
    Collect(Collect),
    Unwind(Unwind),
    Todo(String),
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Scope {
    Tuple = 0,
    Multi = 1,
    Join = 2,
}

impl Algebra {
    /// Recursively calculate the scope.
    /// Note the use of .fold() for cleaner iterator logic.
    pub fn scope(&self) -> Scope {
        match self {
            Algebra::Scan(_) | Algebra::Todo(_) => Scope::Tuple,
            Algebra::Collect(_) | Algebra::Unwind(_) => Scope::Multi,
            Algebra::Project(p) => {
                let expr_max = p.expressions.values()
                    .map(|e| e.scope())
                    .max()
                    .unwrap_or(Scope::Tuple);
                cmp::max(p.input.scope(), expr_max)
            }
            Algebra::Filter(f) => cmp::max(f.input.scope(), f.predicate.scope()),
        }
    }

    /// Fixed the schema recursion logic.
    /// Usually, you want to transform the schema as it moves up the tree.
    pub fn set_schema(&mut self, s: Schema) {
        let input = match self {
            Algebra::Scan(scan) => { scan.schema = s; return; },
            Algebra::Project(p) => &mut p.input,
            Algebra::Filter(f) => &mut f.input,
            Algebra::Collect(c) => &mut c.input,
            Algebra::Unwind(u) => &mut u.input,
            Algebra::Todo(_) => return,
        };
        input.set_schema(s);
    }
}

impl Algebra {
    #[cfg(test)]
    pub(crate) fn project<M: Into<IndexMap<String, Expression>>>(
        child: Algebra,
        expressions: M,
    ) -> Self {
        Algebra::Project(Project {
            expressions: expressions.into(),
            input: Box::new(child),
        })
    }

    #[cfg(test)]
    pub(crate) fn filter(child: Algebra, predicate: Expression) -> Self {
        Algebra::Filter(Filter {
            predicate,
            input: Box::new(child),
        })
    }

    #[cfg(test)]
    pub(crate) fn scan<S: AsRef<str>>(resource: S, schema: Schema) -> Self {
        Algebra::Scan (
            Scan{
                source: resource.as_ref().to_string(),
                schema,
            }
        )
    }

    #[cfg(test)]
    pub(crate) fn unwind<S: AsRef<str>>(child: Algebra, key: S, func: Operator) -> Self {
        Algebra::Unwind(Unwind {
            input: Box::new(child),
            key: key.as_ref().to_string(),
            func,
        })
    }


    pub fn schema(&self) -> Schema {
        Schema::Fixed(IndexMap::from([("price".to_string(), ValType::Float)]))
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
            Algebra::Scan(s) => format!("FROM {}", s.source),
            Algebra::Project(p) => p.sql(),
            Algebra::Filter(f) => f.sql(),
            Algebra::Todo(_) => panic!(),
            Algebra::Collect(_) => panic!(),
            Algebra::Unwind(_) => panic!(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Scan {
    pub source: String,
    pub schema: Schema,
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
    }

    #[test]
    // SELECT Istream(auction, DOLTOEUR(price), bidder, datetime) FROM bid [ROWS UNBOUNDED]
    // simple multiplier
    fn nexmark_q1_cypher() {
        let q1_cypher = "MATCH (n:$$source) RETURN n.auction, n.price * 1.1, n.bidder, n.datetime";
    }

    #[test]
    // SELECT Istream(auction, DOLTOEUR(price), bidder, datetime) FROM bid [ROWS UNBOUNDED]
    // simple multiplier
    fn nexmark_q2_sql() {
        let q1_sql = "SELECT auction, price * 1.1, bidder, datetime FROM $$source";

        let algebra = parse_sql(q1_sql);

        let sql = algebra.sql();
        debug!("{:?}", sql);
    }


}
