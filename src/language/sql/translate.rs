use crate::algebra;
use crate::algebra::AlgebraType::{Filter, Join, Project, Scan};
use crate::algebra::Operator::{Input, Literal, NamedInput, Operation};
use crate::algebra::{AlgebraType, InputFunction, LiteralOperator, NamedRefOperator, Op, OperationFunction, Operator};
use crate::language::sql::statement::{SqlIdentifier, SqlSelect, SqlStatement};
use crate::value::Value;

pub(crate) fn translate(query: SqlStatement) -> Result<AlgebraType, String> {
    let scan = match query {
        SqlStatement::Select(s) => handle_select(s)?,
        _ => Err("Could not translate SQL query".to_string())?
    };
    Ok(scan)
}

fn handle_select(query: SqlSelect) -> Result<AlgebraType, String> {
    let mut sources: Vec<AlgebraType> = query.froms.into_iter().map(|from| handle_from(from).unwrap()).collect();
    let mut projections: Vec<Operator> = query.columns.into_iter().map(|column| handle_field(column).unwrap()).collect();
    let mut filters: Vec<Operator> = query.wheres.into_iter().map(|w| handle_field(w).unwrap()).collect();

    let node = {
        let mut join = sources.remove(0);
        while !sources.is_empty() {
            let right = sources.remove(0);
            join = Join(algebra::Join::new(join, right, |_v| Value::bool(true), |_v| Value::bool(true), |l, r| Value::array(vec![l, r])));
        }
        join
    };

    let node = match filters.len() {
        0 => {
            node
        }
        1 => {
            Filter(algebra::Filter::new(node, filters.pop().unwrap()))
        }
        _ => {
            Filter(algebra::Filter::new(node, Operation(OperationFunction::new(Op::And, filters))))
        }
    };

    let function = match projections.len() {
        1 => {
            let function = projections.pop().unwrap();
            match function {
                Input(_) => {
                    return Ok(node)
                }
                o => o
            }
        }
        _ => {
            Operation(OperationFunction::new(Op::Combine, projections))
        }
    };

    Ok(Project(algebra::Project::new(node, function)))

}

fn handle_from(from: SqlStatement) -> Result<AlgebraType, String> {
    match from {
        SqlStatement::Identifier(i) => handle_table(i),
        _ => Err("Could not translate FROM clause".to_string())
    }
}

fn handle_field(column: SqlStatement) -> Result<Operator, String> {
    match column {
        SqlStatement::Symbol(s) => {
            if s.symbol == "*" {
                Ok(Input(InputFunction::all()))
            } else {
                Err("Could not translate symbol".to_string())
            }
        }
        SqlStatement::Operator(o) => {
            let operators = o.operands.into_iter().map(|op| handle_field(op).unwrap()).collect();
            Ok(Operation(OperationFunction::new(o.operator, operators)))
        }
        SqlStatement::Identifier(i) => {
            match i {
                mut i if i.names.len() == 1 && i.names.get(0).unwrap().starts_with('$') => {
                    let index = i.names.pop().unwrap().clone().replace('$', "");
                    Ok(Input(InputFunction::new(index.parse().unwrap())))
                }
                SqlIdentifier { .. } => {
                    let mut names = i.names.clone();
                    names.remove(0);
                    Ok(NamedInput(NamedRefOperator::new(names.join("."))))
                }
            }
        }
        SqlStatement::Value(v) => {
            Ok(Literal(LiteralOperator::new(v.value)))
        }
        err => Err(format!("Could not translate operator: {:?}", err))
    }
}

fn handle_table(identifier: SqlIdentifier) -> Result<AlgebraType, String> {
    let scan = match identifier.names[0].as_str() {
        s if s.starts_with('$') => s.strip_prefix('$')
            .ok_or("Prefix not found".to_string())
            .and_then(|rest| rest.parse::<i64>().map_err(|_| "Could not parse number".to_string()))
            .map(|num| Scan(algebra::Scan::new(num)))?,
        _ => Err("Could not translate table".to_string())?
    };
    Ok(scan)
}