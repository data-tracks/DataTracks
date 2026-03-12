use sqlparser::dialect::Dialect;
use crate::expression::Expression;
use crate::language::Sql;


#[derive(Clone, Debug)]
pub enum Op {
    // Scalar Ops
    LoadField(usize), // load value from record
    PushConst(usize),
    Add,
    Greater,
    Equal,
    Index,
    Minus,
    Multiply,
    Explode,

    // Explode
    NextOrPop,
    LoadExplodeElement,
    InitExplode(usize),

    // Relational Ops (The Algebra)
    NextRow { table_id: usize }, // holds the "raw" data so that multiple different expressions (filters, math, etc.) can all look at the same row simultaneously without fighting over the stack.
    JumpIfFalse { target: usize }, // jump if top is false
    Jump { target: usize },

    // The "Materialize" Op
    // arg = how many items to pop from stack to form the result row
    Yield(usize),
}



pub enum Algebra {
    S(Scan),
    P(Project),
    F(Filter),
    T(String)
}

impl Sql for Algebra {
    fn sql(&self) -> String {
        match self {
            Algebra::S(s) => s.sql(),
            Algebra::P(p) => p.sql(),
            Algebra::F(f) => f.sql(),
            Algebra::T(_) => panic!()
        }
    }
}


pub struct Scan {
    pub entity: String,
}

impl Sql for Scan {
    fn sql(&self) -> String {
        format!("FROM {}", self.entity)
    }
}

pub struct Project {
    pub expressions: Vec<Expression>,
    pub input: Box<Algebra>,
}

impl Sql for Project {
    fn sql(&self) -> String {
        let select = format!("SELECT {}", self.expressions.iter().map(|e| e.sql()).collect::<Vec<_>>().join(", "));
        let child = self.input.sql();
        format!("{} {}", select, child)
    }
}

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
    use sqlparser::parser::Parser;
    use crate::language::{parse_alg, Sql, StreamDialect};

    #[test]
    // SELECT Istream(auction, DOLTOEUR(price), bidder, datetime) FROM bid [ROWS UNBOUNDED]
    // simple multiplier
    fn nexmark_q1_sql() {
        let q1_sql = "SELECT auction, price * 1.1, bidder, datetime FROM $$source";

        let dialect = StreamDialect {};

        let ast = Parser::parse_sql(&dialect, q1_sql).unwrap();

        println!("{:?}", ast);

        let algebra = parse_alg(ast);

        let sql = algebra.sql();
        println!("{:?}", sql);
    }

    #[test]
    // SELECT Istream(auction, DOLTOEUR(price), bidder, datetime) FROM bid [ROWS UNBOUNDED]
    // simple multiplier
    fn nexmark_q1_mongodb() {
        let q1_mql = "db.$source.aggregate([ $$project: {auction: 1, price: {$multiply: [\"$price\", 1.1]}, bidder, datetime}])";

        let dialect = StreamDialect {};

        let ast = Parser::parse_sql(&dialect, q1_mql).unwrap();

        println!("{:?}", ast);
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
