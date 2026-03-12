use sqlparser::dialect::Dialect;

pub enum Algebra {
    S(Scan),
    P(Project),
    F(Filter),
}

pub struct Scan {}

pub struct Project {}

pub struct Filter {}

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

#[cfg(test)]
mod test {
    use crate::algebra::StreamDialect;
    use sqlparser::parser::Parser;

    #[test]
    // SELECT Istream(auction, DOLTOEUR(price), bidder, datetime) FROM bid [ROWS UNBOUNDED]
    // simple multiplier
    fn nexmark_q1_sql() {
        let q1_sql = "SELECT auction, price * 1.1, bidder, datetime FROM $$source";

        let dialect = StreamDialect {};

        let ast = Parser::parse_sql(&dialect, q1_sql).unwrap();

        println!("{:?}", ast);
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
