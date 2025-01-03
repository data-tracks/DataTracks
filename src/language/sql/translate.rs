use crate::algebra;
use crate::algebra::AlgebraType::{Aggregate, Dual, Filter, IndexScan, Join, Project, TableScan, Variable};
use crate::algebra::Op::Tuple;
use crate::algebra::TupleOp::Input;
use crate::algebra::{AlgebraType, Op, Operator, VariableScan};
use crate::language::sql::statement::SqlStatement::Identifier;
use crate::language::sql::statement::{SqlIdentifier, SqlOperator, SqlSelect, SqlStatement, SqlVariable};
use crate::value::Value;

pub(crate) fn translate(query: SqlStatement) -> Result<AlgebraType, String> {
    let scan = match query {
        SqlStatement::Select(s) => handle_select(s)?,
        _ => Err("Could not translate SQL query".to_string())?
    };
    Ok(scan.alg())
}

fn handle_select(query: SqlSelect) -> Result<MaybeAliasAlg, String> {
    let mut sources: Vec<MaybeAliasAlg> = query.froms.into_iter().map(handle_from).collect::<Result<Vec<_>, _>>()?;
    let aliases = sources.iter().filter(|s| matches!(s, MaybeAliasAlg::Aliased(_))).map(|s| match s {
        MaybeAliasAlg::Aliased(name) => name.name.clone(),
        MaybeAliasAlg::Raw(_) => unreachable!()
    }).collect();
    let mut projections: Vec<Operator> = query.columns.into_iter().map(|column| handle_field(column, &aliases)).collect::<Result<Vec<_>, _>>()?;
    let mut filters: Vec<Operator> = query.wheres.into_iter().map(|w| handle_field(w, &aliases)).collect::<Result<Vec<_>, _>>()?;
    let mut groups: Vec<Operator> = query.groups.into_iter().map(|g| handle_field(g, &aliases)).collect::<Result<Vec<_>, _>>()?;

    let node = {
        if sources.is_empty() {
            Dual(algebra::Dual::new())
        } else {
            let mut join = sources.remove(0).alg();
            while !sources.is_empty() {
                let right = sources.remove(0).alg();
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

    let function = match projections.len() {
        1 => {
            let function = projections.pop().unwrap();
            match function.op {
                Tuple(Input(_)) => {
                    return Ok(MaybeAliasAlg::Raw(node))
                }
                ref _o => function.clone()
            }
        }
        _ => {
            Operator::new(Op::combine(), projections)
        }
    };

    if function.contains_agg() {
        let group = match groups.len() {
            1 => Some(groups.pop().unwrap()),
            0 => None,
            _ => Some(Operator::combine(groups))
        };

        node = Aggregate(algebra::Aggregate::new(Box::new(node), function, group));
        return Ok(MaybeAliasAlg::Raw(node));
    }

    Ok(MaybeAliasAlg::raw(Project(algebra::Project::new(function, node))))
}

fn handle_from(from: SqlStatement) -> Result<MaybeAliasAlg, String> {
    match from {
        Identifier(i) => handle_table(i),
        SqlStatement::Variable(v) => handle_variable(v),
        SqlStatement::Operator(o) => handle_collection_operator(o),
        SqlStatement::Select(s) => handle_select(s),
        err => Err(format!("Could not translate FROM clause: {:?}", err))
    }
}

fn handle_collection_operator(operator: SqlOperator) -> Result<MaybeAliasAlg, String> {
    let op = operator.operator;
    let inputs = operator.operands.into_iter().map(|o| handle_from(o)).collect::<Result<Vec<_>, _>>()?;
    match inputs.len() {
        1 => {
            Ok(MaybeAliasAlg::aliased(op.dump(false).to_lowercase(), AlgebraType::project(Operator::new(op, vec![]), inputs.into_iter().next().unwrap().alg())))
        }
        _ => unreachable!()
    }
}

fn handle_variable(variable: SqlVariable) -> Result<MaybeAliasAlg, String> {
    let inputs = variable.inputs.into_iter().map(|i| handle_from(i).unwrap().alg()).collect();

    Ok(MaybeAliasAlg::aliased(variable.name.clone(), Variable(VariableScan::new(variable.name, inputs))))
}

fn handle_field(column: SqlStatement, aliases: &Vec<String>) -> Result<Operator, String> {
    match column {
        SqlStatement::Symbol(s) => {
            if s.symbol == "*" {
                Ok(Operator::input())
            } else {
                Err("Could not translate symbol".to_string())
            }
        }
        SqlStatement::Operator(o) => {
            let operators = o.operands.into_iter().map(|op| handle_field(op, aliases).unwrap()).collect();
            Ok(Operator::new(o.operator, operators))
        }
        Identifier(i) => {
            let mut names = i.names.clone();
            let mut name = names.remove(0);

            let mut op = if name.starts_with('$') && name.len() > 1 {
                name.remove(0);
                let num = name.parse().unwrap();
                Operator::context(num)
            } else if aliases.contains(&name) {
                if aliases.len() == 1 {
                    Operator::input()
                }else {
                    Operator::index(aliases.iter().position(|s| s == &name).unwrap(), vec![Operator::input()])
                }
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
            let operators = l.list.into_iter().map(|op| handle_field(op, aliases).unwrap()).collect();
            Ok(Operator::new(Op::combine(), operators))
        }
        SqlStatement::Value(v) => {
            Ok(Operator::literal(v.value))
        }
        err => Err(format!("Could not translate operator: {:?}", err))
    }
}

fn handle_table(identifier: SqlIdentifier) -> Result<MaybeAliasAlg, String> {
    let mut names = identifier.names.clone();
    let scan = match names.remove(0) {
        name if name.starts_with('$') => name.strip_prefix('$')
            .ok_or("Prefix not found".to_string())
            .and_then(|rest| rest.parse::<i64>().map_err(|_| "Could not parse number".to_string()))
            .map(|num| IndexScan(algebra::IndexScan::new(num)))?,
        name => TableScan(algebra::TableScan::new(name)),
    };
    if !names.is_empty() {
        let field = handle_field(Identifier(identifier), &vec![])?;
        Ok(MaybeAliasAlg::aliased(names.last().unwrap().clone(), Project(algebra::Project::new(field, scan))))
    } else {
        Ok(MaybeAliasAlg::Raw(scan))
    }
}

enum MaybeAliasAlg {
    Aliased(AliasedAlg),
    Raw(AlgebraType),
}

struct AliasedAlg {
    name: String,
    alg: AlgebraType,
}

impl MaybeAliasAlg {
    fn alg(&self) -> AlgebraType {
        match self {
            MaybeAliasAlg::Aliased(a) => {
                a.alg.clone()
            }
            MaybeAliasAlg::Raw(r) => {
                r.clone()
            }
        }
    }

    fn aliased(name: String, alg: AlgebraType) -> MaybeAliasAlg {
        MaybeAliasAlg::Aliased(AliasedAlg { name, alg })
    }

    fn raw(alg: AlgebraType) -> MaybeAliasAlg {
        MaybeAliasAlg::Raw(alg)
    }
}