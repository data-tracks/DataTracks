use crate::language::statement::Statement;

pub trait Sql: Statement {}

pub(crate) enum SqlStatement {
    Identifier(SqlIdentifier),
    Select(SqlSelect),
    Symbol(SqlSymbol),
}

impl SqlStatement {
    pub(crate) fn dump(&self) -> String {
        match self {
            SqlStatement::Identifier(i) => i.dump(),
            SqlStatement::Select(s) => s.dump(),
            SqlStatement::Symbol(s) => s.dump()
        }
    }
}

pub struct SqlIdentifier {
    pub(crate) names: Vec<String>,
    pub(crate) alias: Option<Box<SqlStatement>>,
}

impl SqlIdentifier {
    pub fn new(names: Vec<String>, alias: Option<SqlStatement>) -> Self {
        SqlIdentifier { names, alias: alias.map(Box::new) }
    }
}

impl Statement for SqlIdentifier {
    fn dump(&self) -> String {
        let mut dump = self.names.join(".");
        if let Some(alias) = &self.alias {
            dump = dump + " AS " + &alias.dump()
        }
        dump
    }
}

impl Sql for SqlIdentifier {}

pub(crate) struct SqlSelect {
    pub(crate) columns: Vec<SqlStatement>,
    pub(crate) froms: Vec<SqlStatement>,
    pub(crate) wheres: Vec<SqlStatement>,
    pub(crate) orders: Vec<SqlStatement>,
    pub(crate) groups: Vec<SqlStatement>,
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
    fn dump(&self) -> String {
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
    fn dump(&self) -> String {
        let mut select = "SELECT ".to_string();
        if let Some(columns) = self.columns.iter().map(|el| el.dump()).reduce(|a, b| a + ", " + &b) {
            select = select + &columns;
        }

        if let Some(froms) = self.froms.iter().map(|el| el.dump()).reduce(|a, b| a + ", " + &b) {
            select = select + " FROM " + &froms;
        }

        select
    }
}

impl Sql for SqlSelect {}