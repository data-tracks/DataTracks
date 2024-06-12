pub trait Statement {}

pub trait Sql: Statement {}

struct SqlIdentifier {
    names: Vec<String>,
    alias: Option<Box<SqlIdentifier>>,
}

pub(crate) struct SqlSelect {
    columns: Vec<Box<dyn Sql>>,
    froms: Vec<Box<dyn Sql>>,
    wheres: Vec<Box<dyn Sql>>,
    orders: Vec<Box<dyn Sql>>,
    groups: Vec<Box<dyn Sql>>,
}

impl SqlSelect {
    pub(crate) fn new(columns: Vec<Box<dyn Sql>>, froms: Vec<Box<dyn Sql>>) -> SqlSelect {
        SqlSelect { columns, froms, wheres: vec![], orders: vec![], groups: vec![] }
    }
}


impl Statement for SqlSelect {}

impl Sql for SqlSelect {}