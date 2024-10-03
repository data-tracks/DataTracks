use crate::algebra;
use crate::algebra::AlgebraType::{Aggregate, Filter, Join, Project, Scan};
use crate::algebra::Op::Tuple;
use crate::algebra::TupleOp::Input;
use crate::algebra::{AlgebraType, Op, Operator, Replaceable};
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
    let mut groups: Vec<Operator> = query.groups.into_iter().map(|g| handle_field(g).unwrap()).collect();


    let node = {
        let mut join = sources.remove(0);
        while !sources.is_empty() {
            let right = sources.remove(0);
            join = Join(algebra::Join::new(join, right, |_v| Value::bool(true), |_v| Value::bool(true), |l, r| Value::array(vec![l, r])));
        }
        join
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

    let aggs = function.replace(|o| {
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

    if !aggs.is_empty() || !groups.is_empty() {
        let group = match groups.len() {
            1 => Some(groups.pop().unwrap()),
            0 => None,
            _ => Some(Operator::combine(groups))
        };

        node = Aggregate(algebra::Aggregate::new(Box::new(node), aggs, group));
        return Ok(node);
    }

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
                Ok(Operator::input())
            } else {
                Err("Could not translate symbol".to_string())
            }
        }
        SqlStatement::Operator(o) => {
            let operators = o.operands.into_iter().map(|op| handle_field(op).unwrap()).collect();
            Ok(Operator::new(o.operator, operators))
        }
        SqlStatement::Identifier(i) => {
            let names = i.names.clone();
            Ok(Operator::name(&names.join(".")))
        }
        SqlStatement::Value(v) => {
            Ok(Operator::literal(v.value))
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