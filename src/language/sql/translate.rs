use crate::algebra;
use crate::algebra::AlgebraType::{Aggregate, Dual, Filter, IndexScan, Join, Project, TableScan, Variable};
use crate::algebra::Op::Tuple;
use crate::algebra::TupleOp::Input;
use crate::algebra::{AlgebraType, Op, Operator, Replaceable, VariableScan};
use crate::language::sql::statement::SqlStatement::Identifier;
use crate::language::sql::statement::{SqlIdentifier, SqlOperator, SqlSelect, SqlStatement, SqlVariable};
use crate::value::Value;

pub(crate) fn translate(query: SqlStatement) -> Result<AlgebraType, String> {
    let scan = match query {
        SqlStatement::Select(s) => handle_select(s)?,
        _ => Err("Could not translate SQL query".to_string())?
    };
    Ok(scan)
}

fn handle_select(query: SqlSelect) -> Result<AlgebraType, String> {
    let mut sources: Vec<AlgebraType> = query.froms.into_iter().map(|from| handle_from(from)).collect::<Result<Vec<_>,_>>()?;
    let mut projections: Vec<Operator> = query.columns.into_iter().map(|column| handle_field(column)).collect::<Result<Vec<_>,_>>()?;
    let mut filters: Vec<Operator> = query.wheres.into_iter().map(|w| handle_field(w)).collect::<Result<Vec<_>,_>>()?;
    let mut groups: Vec<Operator> = query.groups.into_iter().map(|g| handle_field(g)).collect::<Result<Vec<_>,_>>()?;

    let node = {
        if sources.is_empty() {
            Dual(algebra::Dual::new())
        }else {
            let mut join = sources.remove(0);
            while !sources.is_empty() {
                let right = sources.remove(0);
                join = Join(algebra::Join::new(join, right, |_v| Value::bool(true), |_v| Value::bool(true), |l, r| {
                    Value::array(vec![l, r])
                }));
            }
            join
        }

    };

    let mut node = match filters.len() {
        0 => {
            node
        }
        1 => {
            Filter(algebra::Filter::new(node, filters.pop().unwrap()))
        }
        _ => {
            Filter(algebra::Filter::new(node, Operator::new(Op::and(), filters)))
        }
    };

    let mut function = match projections.len() {
        1 => {
            let function = projections.pop().unwrap();
            match function.op {
                Tuple(Input(_)) => {
                    return Ok(node)
                }
                ref _o => function.clone()
            }
        }
        _ => {
            Operator::new(Op::combine(), projections)
        }
    };

    let aggregations = function.replace(|o| {
        // we replace the operator
        match &o.op {
            Op::Agg(a) => {
                let replaced = (a.clone(), o.operands.clone());

                o.op = Op::input();
                o.operands = vec![];

                vec![replaced]
            }
            _ => vec![]
        }
    });

    if !aggregations.is_empty() || !groups.is_empty() {
        let group = match groups.len() {
            1 => Some(groups.pop().unwrap()),
            0 => None,
            _ => Some(Operator::combine(groups))
        };

        node = Aggregate(algebra::Aggregate::new(Box::new(node), aggregations, group));
        return Ok(node);
    }

    Ok(Project(algebra::Project::new(function, node)))
}

fn handle_from(from: SqlStatement) -> Result<AlgebraType, String> {
    match from {
        Identifier(i) => handle_table(i),
        SqlStatement::Variable(v) => handle_variable(v),
        SqlStatement::Operator(o) => handle_collection_operator(o),
        _ => Err("Could not translate FROM clause".to_string())
    }
}

fn handle_collection_operator(operator: SqlOperator) -> Result<AlgebraType, String> {
    let op = operator.operator;
    let inputs = operator.operands.into_iter().map(|o| handle_from(o)).collect::<Result<Vec<_>,_>>()?;
    match inputs.len() {
        1 => {
            Ok(AlgebraType::project(Operator::new(op, vec![]), inputs.into_iter().next().unwrap()))
        }
        _ => unreachable!()
    }
}

fn handle_variable(variable: SqlVariable) -> Result<AlgebraType, String> {
    let inputs = variable.inputs.into_iter().map(|i| handle_from(i).unwrap()).collect();

    Ok(Variable(VariableScan::new(variable.name, inputs)))
}

fn handle_field(column: SqlStatement) -> Result<Operator, String> {
    match column {
        SqlStatement::Symbol(s) => {
            if s.symbol == "*" {
                Ok(Operator::input())
            } else {
                Err("Could not translate symbol".to_string())
            }
        }
        SqlStatement::Operator(o) => {
            let operators = o.operands.into_iter().map(|op| handle_field(op).unwrap()).collect();
            Ok(Operator::new(o.operator, operators))
        }
        Identifier(i) => {
            let mut names = i.names.clone();
            let mut name = names.remove(0);

            let mut op = if name.starts_with('$') && name.len() > 1 {
                name.remove(0);
                let num = name.parse().unwrap();
                Operator::context(num)
            } else {
                Operator::name(&name, vec![])
            };

            for name in names {
                if let Ok(num) = name.parse() {
                    op = Operator::index(num, vec![op]);
                    continue;
                }

                op = Operator::name(&name, vec![op]);
            }

            Ok(op)
        }
        SqlStatement::List(l) => {
            let operators = l.list.into_iter().map(|op| handle_field(op).unwrap()).collect();
            Ok(Operator::new(Op::combine(), operators))
        },
        SqlStatement::Value(v) => {
            Ok(Operator::literal(v.value))
        }
        err => Err(format!("Could not translate operator: {:?}", err))
    }
}

fn handle_table(identifier: SqlIdentifier) -> Result<AlgebraType, String> {
    let mut names = identifier.names.clone();
    let scan = match names.remove(0) {
        name if name.starts_with('$') => name.strip_prefix('$')
            .ok_or("Prefix not found".to_string())
            .and_then(|rest| rest.parse::<i64>().map_err(|_| "Could not parse number".to_string()))
            .map(|num| IndexScan(algebra::IndexScan::new(num)))?,
        name => TableScan(algebra::TableScan::new(name)),
    };
    if !names.is_empty() {
        let field = handle_field(Identifier(identifier))?;
        Ok(Project(algebra::Project::new(field, scan)))
    } else {
        Ok(scan)
    }

}