use crate::algebra::AlgebraType::{Project, Scan};
use crate::algebra::Function::{Input, Literal, NamedInput, Operation};
use crate::algebra::{AlgebraType, CombineOperator, Function, InputFunction, LiteralOperator, NamedRefOperator, OperationFunction, Operator, TrainProject, TrainScan};
use crate::language::sql::statement::{SqlIdentifier, SqlSelect, SqlStatement};
use crate::language::statement::Statement;

pub(crate) fn translate(query: SqlStatement) -> Result<AlgebraType, String> {
    let scan = match query {
        SqlStatement::Select(s) => handle_select(s)?,
        _ => Err("Could not translate SQL query".to_string())?
    };
    Ok(scan)
}

fn handle_select(query: SqlSelect) -> Result<AlgebraType, String> {
    let mut sources: Vec<AlgebraType> = query.froms.into_iter().map(|from| handle_from(from).unwrap()).collect();
    let mut functions: Vec<Function> = query.columns.into_iter().map(|column| handle_column(column).unwrap()).collect();

    let function = match functions.len() {
        1 => {
            let function = functions.pop().unwrap();
            match function {
                Input(_) => {
                    return Ok(sources.pop().unwrap())
                }
                o => o
            }
        }
        _ => {
            Operation(OperationFunction::new(Operator::Combine(CombineOperator {}), functions))
        }
    };

    let project = Project(TrainProject::new(sources.pop().unwrap(), function));

    Ok(project)
}

fn handle_from(from: SqlStatement) -> Result<AlgebraType, String> {
    match from {
        SqlStatement::Identifier(i) => handle_table(i),
        _ => Err("Could not translate FROM clause".to_string())
    }
}

fn handle_column(column: SqlStatement) -> Result<Function, String> {
    match column {
        SqlStatement::Symbol(s) => {
            if s.symbol == "*" {
                Ok(Input(InputFunction::new()))
            } else {
                Err("Could not translate symbol".to_string())
            }
        }
        SqlStatement::Operator(o) => {
            let operators = o.operands.into_iter().map(|op| handle_column(op).unwrap()).collect();
            Ok(Operation(OperationFunction::new(o.operator, operators)))
        }
        SqlStatement::Identifier(i) => {
            Ok(NamedInput(NamedRefOperator::new(i.dump(""))))
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
            .map(|num| Scan(TrainScan::new(num)))?,
        _ => Err("Could not translate table".to_string())?
    };
    Ok(scan)
}