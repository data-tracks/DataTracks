use crate::algebra::Operator;
use crate::language::statement::Statement;
use crate::value;
use crate::value::Value;

pub trait Sql: Statement {}

pub(crate) enum SqlStatement {
    Identifier(SqlIdentifier),
    Select(SqlSelect),
    Symbol(SqlSymbol),
    List(SqlList),
    Value(SqlValue),
    Operator(SqlOperator),
    Alias(SqlAlias),
}

impl SqlStatement {
    pub(crate) fn dump(&self, quote: &str) -> String {
        match self {
            SqlStatement::Identifier(i) => i.dump(quote),
            SqlStatement::Select(s) => s.dump(quote),
            SqlStatement::Symbol(s) => s.dump(quote),
            SqlStatement::List(s) => s.dump(quote),
            SqlStatement::Value(s) => s.dump(quote),
            SqlStatement::Operator(s) => s.dump(quote),
            SqlStatement::Alias(s) => s.dump(quote),
        }
    }
}

pub struct SqlIdentifier {
    pub(crate) names: Vec<String>,
}

impl SqlIdentifier {
    pub fn new(names: Vec<String>) -> Self {
        SqlIdentifier { names }
    }
}

impl Statement for SqlIdentifier {
    fn dump(&self, quote: &str) -> String {
        let mut dump = format!("{}", self.names.join("."));
        dump
    }
}

pub struct SqlAlias {
    pub(crate) target: Box<SqlStatement>,
    pub(crate) alias: Box<SqlStatement>,
}

impl SqlAlias {
    pub fn new(target: SqlStatement, alias: SqlStatement) -> Self {
        SqlAlias { target: Box::new(target), alias: Box::new(alias) }
    }
}

impl Statement for SqlAlias {
    fn dump(&self, quote: &str) -> String {
        let mut dump = format!("{} AS {}", self.target.dump(quote), self.alias.dump(quote));
        dump
    }
}

pub struct SqlOperator {
    pub(crate) operator: Operator,
    pub(crate) operands: Vec<SqlStatement>,
    pub(crate) is_call: bool // call: Function(op1, op2), no call: op1 op op2
}

impl SqlOperator {
    pub fn new(operator: Operator, operands: Vec<SqlStatement>, is_call: bool) -> Self {
        SqlOperator { operator, operands, is_call }
    }
}

impl Statement for SqlOperator {
    fn dump(&self, quote: &str) -> String {

        if self.is_call {
            let op = self.operator.dump(true);
            return format!("{}({})", op, self.operands.iter().map(|o| o.dump(quote)).collect::<Vec<String>>().join(", "))
        }
        let op = self.operator.dump(false);
        match self.operands.len() {
            1 => {
                format!("{}{}", op, self.operands.first().unwrap().dump(quote))
            }
            2 => {
                format!("{} {} {}", self.operands.first().unwrap().dump(quote), op, self.operands.get(1).unwrap().dump(quote))
            }
            _ => {
                self.operands.iter().fold(String::new(), |a, b| format!("{} {} {}", a, op, b.dump(quote)))
            }
        }
    }
}

pub struct SqlValue {
    pub(crate) value: Value,
}



impl SqlValue {
    pub fn new(value: value::Value) -> Self {
        SqlValue { value }
    }
}

impl Statement for SqlValue {
    fn dump(&self, quote: &str) -> String {
        format!("{}", self.value)
    }
}

pub(crate) struct SqlSelect {
    pub(crate) columns: Vec<SqlStatement>,
    pub(crate) froms: Vec<SqlStatement>,
    pub(crate) wheres: Vec<SqlStatement>,
    pub(crate) orders: Vec<SqlStatement>,
    pub(crate) groups: Vec<SqlStatement>,
}

pub(crate) struct SqlList {
    list: Vec<SqlStatement>,
}

impl SqlList {
    pub fn new(list: Vec<SqlStatement>) -> Self {
        SqlList { list }
    }
}

impl Statement for SqlList {
    fn dump(&self, quote: &str) -> String {
        self.list.iter().map(|a| a.dump(quote)).fold(String::from(""), |a, b| a + &b)
    }
}


pub(crate) struct SqlSymbol {
    symbol: String,
}

impl SqlSymbol {
    pub(crate) fn new(symbol: &str) -> SqlSymbol {
        SqlSymbol { symbol: symbol.to_string() }
    }
}

impl Statement for SqlSymbol {
    fn dump(&self, _quote: &str) -> String {
        self.symbol.to_string()
    }
}

impl Sql for SqlSymbol {}


impl SqlSelect {
    pub(crate) fn new(columns: Vec<SqlStatement>, froms: Vec<SqlStatement>) -> SqlSelect {
        SqlSelect { columns, froms, wheres: vec![], orders: vec![], groups: vec![] }
    }
}


impl Statement for SqlSelect {
    fn dump(&self, quote: &str) -> String {
        let mut select = "SELECT ".to_string();
        if let Some(columns) = self.columns.iter().map(|el| el.dump(quote)).reduce(|a, b| a + ", " + &b) {
            select = select + &columns;
        }

        if let Some(froms) = self.froms.iter().map(|el| el.dump("")).reduce(|a, b| a + ", " + &b) {
            select = select + " FROM " + &froms;
        }

        select
    }
}

impl Sql for SqlSelect {}