use crate::language::statement::Statement;

pub trait Sql: Statement {}

pub struct SqlIdentifier {
    names: Vec<String>,
    alias: Option<Box<dyn Sql>>,
}

impl SqlIdentifier {
    pub fn new(names: Vec<String>, alias: Option<Box<dyn Sql>>) -> Self {
        SqlIdentifier { names, alias }
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
    columns: Vec<Box<dyn Sql>>,
    froms: Vec<Box<dyn Sql>>,
    wheres: Vec<Box<dyn Sql>>,
    orders: Vec<Box<dyn Sql>>,
    groups: Vec<Box<dyn Sql>>,
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
    pub(crate) fn new(columns: Vec<Box<dyn Sql>>, froms: Vec<Box<dyn Sql>>) -> SqlSelect {
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