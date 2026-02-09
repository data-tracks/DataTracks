use std::error::Error;
use bytes::{BufMut, BytesMut};
use postgres::types::{IsNull, Type};
use speedy::Writable;
use crate::{bool};
use crate::value::Value;

impl<'a> postgres::types::FromSql<'a> for Value {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match *ty {
            Type::BOOL => Ok(Value::bool(postgres::types::FromSql::from_sql(ty, raw)?)),
            Type::TEXT | Type::CHAR => {
                Ok(Value::text(postgres::types::FromSql::from_sql(ty, raw)?))
            }
            Type::INT2 | Type::INT4 | Type::INT8 => {
                Ok(Value::int(postgres::types::FromSql::from_sql(ty, raw)?))
            }
            Type::FLOAT4 | Type::FLOAT8 => {
                Ok(Value::float(postgres::types::FromSql::from_sql(ty, raw)?))
            }
            _ => Err(format!("Unrecognized value type: {}", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::TEXT
                | Type::CHAR
                | Type::BOOL
                | Type::INT2
                | Type::INT8
                | Type::INT4
                | Type::FLOAT4
                | Type::FLOAT8
        )
    }
}

impl postgres::types::ToSql for Value {
    fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            Value::Int(i) => out.put_i32(i.0 as i32),
            Value::Float(f) => out.put_f64(f.as_f64()),
            Value::Bool(b) => out.extend_from_slice(&[b.0 as u8]),
            Value::Text(t) => out.extend_from_slice(t.0.as_bytes()),
            Value::Array(_) => return Err("Array not supported".into()),
            Value::Dict(_) => return Err("Dict not supported".into()),
            Value::Null => return Ok(IsNull::Yes),
            Value::Time(t) => out.put_i128(t.ms as i128),
            Value::Date(d) => out.put_i64(d.days),
            Value::Node(n) => out.extend_from_slice(n.write_to_vec()?.as_slice()),
            Value::Edge(e) => out.extend_from_slice(e.write_to_vec()?.as_slice()),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        matches!(
            *ty,
            Type::TEXT
                | Type::BOOL
                | Type::INT8
                | Type::INT4
                | Type::INT2
                | Type::FLOAT4
                | Type::FLOAT8
        )
    }

    postgres::types::to_sql_checked!();
}

impl From<postgres::Row> for Value {
    fn from(row: postgres::Row) -> Self {
        let len = row.len();
        let mut values = Vec::with_capacity(len);
        for i in 0..len {
            values.push(row.get::<usize, Value>(i));
        }
        if values.len() == 1 {
            values.pop().unwrap()
        } else {
            Value::array(values)
        }
    }
}