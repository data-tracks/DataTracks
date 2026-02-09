use rusqlite::types::{FromSqlResult, ToSqlOutput, ValueRef};
use speedy::Writable;
use crate::value::Value;

impl TryFrom<(&rusqlite::Row<'_>, usize)> for Value {
    type Error = rusqlite::Error;

    fn try_from(pair: (&rusqlite::Row<'_>, usize)) -> Result<Self, Self::Error> {
        let row = pair.0;
        let mut values = Vec::with_capacity(pair.1);
        for i in 0..pair.1 {
            let value_ref = row.get_ref(i)?;
            values.push(rusqlite::types::FromSql::column_result(value_ref)?);
        }
        if values.len() == 1 {
            Ok(values.pop().unwrap())
        } else {
            Ok(Value::array(values))
        }
    }
}

impl rusqlite::types::FromSql for Value {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.data_type() {
            rusqlite::types::Type::Null => Ok(Value::null()),
            rusqlite::types::Type::Integer => Ok(Value::int(value.as_i64()?)),
            rusqlite::types::Type::Real => Ok(Value::float(value.as_f64()?)),
            rusqlite::types::Type::Text => Ok(Value::text(value.as_str()?)),
            rusqlite::types::Type::Blob => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

impl rusqlite::types::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Value::Int(i) => Ok(ToSqlOutput::from(i.0)),
            Value::Float(f) => Ok(ToSqlOutput::from(f.as_f64())),
            Value::Bool(b) => Ok(ToSqlOutput::from(b.0)),
            Value::Text(t) => Ok(ToSqlOutput::from(t.0.clone())),
            Value::Time(t) => Ok(ToSqlOutput::from(t.ms)),
            Value::Array(_) => Err(rusqlite::Error::InvalidQuery),
            Value::Dict(_) => Err(rusqlite::Error::InvalidQuery),
            Value::Null => Ok(ToSqlOutput::from(rusqlite::types::Null)),
            Value::Date(d) => Ok(ToSqlOutput::from(d.days)),
            Value::Node(n) => Ok(ToSqlOutput::from(n.write_to_vec().unwrap())),
            Value::Edge(e) => Ok(ToSqlOutput::from(e.write_to_vec().unwrap())),
        }
    }
}