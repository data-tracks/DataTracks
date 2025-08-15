use crate::language::statement::Statement;
use std::ops::Deref;
use value::Value;

pub trait Mql: Statement {}

#[derive(Debug, Clone)]
pub enum MqlStatement {
    Insert(MqlInsert),
    Find(MqlFind),
    Delete(MqlDelete),
    Update(MqlUpdate),
    Dynamic(MqlDynamic),
    Identifier(MqlIdentifier),
    Value(MqlValue),
}

impl Deref for MqlStatement {
    type Target = dyn Statement;

    fn deref(&self) -> &Self::Target {
        match self {
            MqlStatement::Insert(i) => i,
            MqlStatement::Find(f) => f,
            MqlStatement::Delete(d) => d,
            MqlStatement::Update(u) => u,
            MqlStatement::Dynamic(d) => d,
            MqlStatement::Identifier(i) => i,
            MqlStatement::Value(v) => v,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MqlIdentifier {
    pub name: String,
}

impl Statement for MqlIdentifier {
    fn dump(&self, quote: &str) -> String {
        format!("{}{}{}", quote, self.name, quote)
    }
}

#[derive(Debug, Clone)]
pub struct MqlInsert {
    pub(crate) collection: String,
    pub(crate) values: Box<MqlStatement>,
}

impl Statement for MqlInsert {
    fn dump(&self, _quote: &str) -> String {
        format!("db.{}.insert({})", self.collection, self.values.dump("\""))
    }
}

#[derive(Debug, Clone)]
pub struct MqlDelete {}

impl Statement for MqlDelete {
    fn dump(&self, quote: &str) -> String {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct MqlUpdate {}

impl Statement for MqlUpdate {
    fn dump(&self, quote: &str) -> String {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct MqlFind {}

impl Statement for MqlFind {
    fn dump(&self, quote: &str) -> String {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct MqlValue {
    pub(crate) value: Value,
}

impl Statement for MqlValue {
    fn dump(&self, quote: &str) -> String {
        Self::dump_value(&self.value, quote)
    }
}

impl MqlValue {
    fn dump_value(value: &Value, quote: &str) -> String {
        match value {
            Value::Text(t) => {
                format!("{quote}{t}{quote}")
            }
            Value::Array(a) => {
                format!(
                    "[{}]",
                    a.values
                        .iter()
                        .map(|v| Self::dump_value(v, quote))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            Value::Dict(d) => {
                format!(
                    "{{{}}}",
                    d.iter()
                        .map(|(k, v)| format!("{quote}{k}{quote}:{}", Self::dump_value(v, quote)))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            Value::Wagon(w) => {
                let value = w.clone().unwrap();
                Self::dump_value(&value, quote)
            }
            v => format!("{v}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MqlDynamic {
    pub id: String, // $0 or $name
}

impl Statement for MqlDynamic {
    fn dump(&self, _quote: &str) -> String {
        format!("${}", self.id)
    }
}
