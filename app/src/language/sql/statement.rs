use crate::algebra::{Op, TupleOp};
use crate::language::statement::Statement;
use crate::util::{TimeUnit, WindowType};
use crate::TriggerType;
use std::fmt::Display;
use value::{ValType, Value};

pub trait Sql: Statement {}

#[derive(Debug)]
pub(crate) enum SqlStatement {
    Identifier(SqlIdentifier),
    Select(SqlSelect),
    Symbol(SqlSymbol),
    List(SqlList),
    Value(SqlValue),
    Type(SqlType),
    Operator(SqlOperator),
    Alias(SqlAlias),
    Variable(SqlVariable),
    Window(SqlWindow),
    Trigger(SqlTrigger),
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
            SqlStatement::Variable(v) => v.dump(quote),
            SqlStatement::Type(t) => t.dump(quote),
            SqlStatement::Window(t) => t.dump(quote),
            SqlStatement::Trigger(t) => t.dump(quote),
        }
    }

    pub(crate) fn as_literal(&self) -> Option<Value> {
        match self {
            SqlStatement::Value(v) => Some(v.value.clone()),
            SqlStatement::Identifier(i) => Some(Value::text(&i.names.join("."))),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqlWindow {
    _type: WindowType,
    size: Option<(usize, TimeUnit)>,
    offset: Option<(isize, TimeUnit)>,
}

impl SqlWindow {
    pub fn new(
        _type: WindowType,
        size: Option<(usize, TimeUnit)>,
        offset: Option<(isize, TimeUnit)>,
    ) -> Self {
        SqlWindow {
            _type,
            size,
            offset,
        }
    }

    fn dump(&self, quote: &str) -> String {
        let mut query = format!("{}", self._type);
        let mut elements = vec![];
        if let Some((size, unit)) = &self.size {
            elements.push(("SIZE", (size.to_string(), unit)));
        }

        if let Some((offset, unit)) = &self.offset {
            elements.push(("OFFSET", (offset.to_string(), unit)));
        }

        if !elements.is_empty() {
            query += "(";
            query += &elements
                .iter()
                .map(|(id, (amount, unit))| format!("{} {} {}", id, amount, unit.dump_full(quote)))
                .collect::<Vec<_>>()
                .join(", ");
            query += ")";
        }

        query
    }
}

#[derive(Debug, Clone)]
pub struct SqlTrigger {
    type_: TriggerType,
}

impl SqlTrigger {
    pub fn new(type_: TriggerType) -> Self {
        SqlTrigger { type_ }
    }

    pub fn dump(&self, quote: &str) -> String {
        self.type_.dump(quote)
    }
}

impl Display for SqlTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EMIT {}", self.type_)
    }
}

#[derive(Debug)]
pub struct SqlType {
    pub sql_type: ValType,
}

impl SqlType {
    fn dump(&self, quote: &str) -> String {
        self.sql_type.dump(quote)
    }
    pub(crate) fn new(sql_type: ValType) -> SqlType {
        SqlType { sql_type }
    }
}

#[derive(Debug)]
pub struct SqlVariable {
    pub inputs: Vec<SqlStatement>,
    pub(crate) name: String,
}

impl SqlVariable {
    pub(crate) fn new(name: String, inputs: Vec<SqlStatement>) -> Self {
        SqlVariable { name, inputs }
    }
}

impl Statement for SqlVariable {
    fn dump(&self, quote: &str) -> String {
        format!("{}${}{}", quote, self.name, quote)
    }
}

#[derive(Debug)]
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
        self.names
            .iter()
            .map(|n| format!("{}{}{}", quote, n, quote))
            .collect::<Vec<_>>()
            .join(".")
            .to_string()
    }
}

#[derive(Debug)]
pub struct SqlAlias {
    pub(crate) target: Box<SqlStatement>,
    pub(crate) alias: Box<SqlStatement>,
}

impl SqlAlias {
    pub fn new(target: SqlStatement, alias: SqlStatement) -> Self {
        SqlAlias {
            target: Box::new(target),
            alias: Box::new(alias),
        }
    }
}

impl Statement for SqlAlias {
    fn dump(&self, quote: &str) -> String {
        format!("{} AS {}", self.target.dump(quote), self.alias.dump(quote))
    }
}

#[derive(Debug)]
pub struct SqlOperator {
    pub(crate) operator: Op,
    pub(crate) operands: Vec<SqlStatement>,
    pub(crate) is_call: bool, // call: Function(op1, op2), no call: op1 op op2
}

impl SqlOperator {
    pub fn new(operator: Op, operands: Vec<SqlStatement>, is_call: bool) -> Self {
        SqlOperator {
            operator,
            operands,
            is_call,
        }
    }
}

impl Statement for SqlOperator {
    fn dump(&self, quote: &str) -> String {
        // special cases
        if self.is_call {
            let op = self.operator.dump(true);
            return format!(
                "{}({})",
                op,
                self.operands
                    .iter()
                    .map(|o| o.dump(quote))
                    .collect::<Vec<String>>()
                    .join(", ")
            );
        } else if matches!(self.operator, Op::Tuple(TupleOp::Doc)) {
            let operators = self
                .operands
                .iter()
                .map(|o| o.dump(quote))
                .collect::<Vec<String>>()
                .join(", ");
            return format!("{{{}}}", operators);
        } else if matches!(self.operator, Op::Tuple(TupleOp::KeyValue(_))) {
            return format!(
                "{}:{}",
                self.operands.first().unwrap().dump(quote),
                self.operands.get(1).unwrap().dump(quote)
            );
        }

        let op = self.operator.dump(false);
        match self.operands.len() {
            1 => {
                format!("{}{}", op, self.operands.first().unwrap().dump(quote))
            }
            2 => {
                format!(
                    "{} {} {}",
                    self.operands.first().unwrap().dump(quote),
                    op,
                    self.operands.get(1).unwrap().dump(quote)
                )
            }
            _ => self.operands.iter().fold(String::new(), |a, b| {
                format!("{} {} {}", a, op, b.dump(quote))
            }),
        }
    }
}

#[derive(Debug)]
pub struct SqlValue {
    pub(crate) value: Value,
}

impl SqlValue {
    pub fn new(value: Value) -> Self {
        SqlValue { value }
    }

    fn dump_value(value: &Value, quote: &str) -> String {
        match value {
            Value::Text(t) => {
                format!("{}{}{}", quote, t, quote)
            }
            Value::Wagon(w) => {
                let value = w.clone().unwrap();
                Self::dump_value(&value, quote)
            }
            v => format!("{}", v),
        }
    }
}

impl Statement for SqlValue {
    fn dump(&self, _quote: &str) -> String {
        SqlValue::dump_value(&self.value, "'")
    }
}

#[derive(Debug)]
pub(crate) struct SqlSelect {
    pub(crate) columns: Vec<SqlStatement>,
    pub(crate) froms: Vec<SqlStatement>,
    pub(crate) window: Option<SqlWindow>,
    pub(crate) wheres: Vec<SqlStatement>,
    pub(crate) orders: Vec<SqlStatement>,
    pub(crate) groups: Vec<SqlStatement>,
    pub(crate) trigger: Option<SqlTrigger>,
}

#[derive(Debug)]
pub(crate) struct SqlList {
    pub list: Vec<SqlStatement>,
}

impl SqlList {
    pub fn new(list: Vec<SqlStatement>) -> Self {
        SqlList { list }
    }
}

impl Statement for SqlList {
    fn dump(&self, quote: &str) -> String {
        self.list
            .iter()
            .map(|a| a.dump(quote))
            .fold(String::from(""), |a, b| a + &b)
    }
}

#[derive(Debug)]
pub(crate) struct SqlSymbol {
    pub symbol: String,
}

impl SqlSymbol {
    pub(crate) fn new(symbol: &str) -> SqlSymbol {
        SqlSymbol {
            symbol: symbol.to_string(),
        }
    }
}

impl Statement for SqlSymbol {
    fn dump(&self, _quote: &str) -> String {
        self.symbol.to_string()
    }
}

impl Sql for SqlSymbol {}

impl SqlSelect {
    pub(crate) fn new(
        columns: Vec<SqlStatement>,
        froms: Vec<SqlStatement>,
        window: Option<SqlWindow>,
        wheres: Vec<SqlStatement>,
        groups: Vec<SqlStatement>,
        trigger: Option<SqlTrigger>,
    ) -> SqlSelect {
        SqlSelect {
            columns,
            froms,
            window,
            wheres,
            orders: vec![],
            groups,
            trigger,
        }
    }
}

impl Statement for SqlSelect {
    fn dump(&self, quote: &str) -> String {
        let mut select = "SELECT ".to_string();
        if let Some(columns) = self
            .columns
            .iter()
            .map(|el| el.dump(quote))
            .reduce(|a, b| a + ", " + &b)
        {
            select += &columns;
        }

        if let Some(froms) = self
            .froms
            .iter()
            .map(|el| el.dump(""))
            .reduce(|a, b| a + ", " + &b)
        {
            select += format!(" FROM {}", &froms).as_str();
        }

        if let Some(window) = self.window.clone() {
            select += format!(" WINDOW {}", window.dump(quote)).as_str();
        }

        if !self.wheres.is_empty() {
            select += format!(
                " WHERE {}",
                self.wheres
                    .iter()
                    .map(|el| el.dump(quote))
                    .collect::<Vec<String>>()
                    .join(" AND ")
            )
            .as_str();
        }

        if let Some(trigger) = self.trigger.clone() {
            select += format!(" EMIT {}", trigger.dump(quote)).as_str();
        }

        select
    }
}

impl Sql for SqlSelect {}
