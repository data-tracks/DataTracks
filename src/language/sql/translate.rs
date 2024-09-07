use crate::algebra::AlgebraType::Scan;
use crate::algebra::{AlgebraType, TrainScan};
use crate::language::sql::statement::{SqlIdentifier, SqlSelect, SqlStatement};

pub(crate) fn translate(query: SqlStatement) -> Result<AlgebraType, String> {
    let mut scans = match query {
        SqlStatement::Select(s) => handle_select(s)?,
        _ => Err("Could not translate SQL query".to_string())?
    };

    if scans.len() == 1 {
        return Ok(scans.pop().unwrap())
    }


    Err("Not supported.".to_string())
}

fn handle_select(query: SqlSelect) -> Result<Vec<AlgebraType>, String> {
    let sources = query.froms.into_iter().map(|from| handle_from(from).unwrap()).collect();
    Ok(sources)
}

fn handle_from(from: SqlStatement) -> Result<AlgebraType, String> {
    match from {
        SqlStatement::Identifier(i) => handle_table(i),
        _ => Err("Could not translate FROM clause".to_string())
    }
}

fn handle_table(identifier: SqlIdentifier) -> Result<AlgebraType, String> {
    let scan = match identifier.names[0].as_str() {
        s if s.starts_with('$') => s.strip_prefix('$')
            .ok_or("Prefix not found".to_string())
            .and_then(|rest| rest.parse::<i64>().map_err(|_| "Could not parse number".to_string()))
            .map(|num| Scan(TrainScan::new(num)))?,
        _ => Err("Could not translate table".to_string())?
    };
    Ok(scan)
}