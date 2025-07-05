use crate::algebra::Op::Tuple;
use crate::algebra::TupleOp::Input;
use crate::algebra::{AlgebraRoot, Algebraic, Op, Operator};
use crate::language::sql::statement::SqlStatement::Identifier;
use crate::language::sql::statement::{
    SqlIdentifier, SqlOperator, SqlSelect, SqlStatement, SqlVariable,
};

pub(crate) fn translate(query: SqlStatement) -> Result<AlgebraRoot, String> {
    let alg = match query {
        SqlStatement::Select(s) => handle_select(s)?,
        _ => Err("Could not translate SQL query".to_string())?,
    };
    Ok(alg)
}

fn handle_select(query: SqlSelect) -> Result<AlgebraRoot, String> {
    let root: Option<AlgebraRoot> = query
        .froms
        .into_iter()
        .map(handle_from)
        .map(|r| r.unwrap())
        .collect();

    let mut root = root.unwrap_or_else(|| AlgebraRoot::dual());

    let aliases = root.aliases();

    let mut projections: Vec<Operator> = query
        .columns
        .into_iter()
        .map(|column| handle_field(column, &aliases))
        .collect::<Result<Vec<_>, _>>()?;
    let mut filters: Vec<Operator> = query
        .wheres
        .into_iter()
        .map(|w| handle_field(w, &aliases))
        .collect::<Result<Vec<_>, _>>()?;
    let mut groups: Vec<Operator> = query
        .groups
        .into_iter()
        .map(|g| handle_field(g, &aliases))
        .collect::<Result<Vec<_>, _>>()?;

    if root.ends().len() > 1 {
        root.join_cross();
    }

    // add filter
    match filters.len() {
        0 => {}
        1 => root.filter(filters.pop().unwrap()),
        _ => root.filter(Operator::new(Op::and(), filters)),
    };

    let function = match projections.len() {
        1 => {
            let function = projections.pop().unwrap();
            match function.op {
                Tuple(Input(_)) => return Ok(root),
                ref _o => function.clone(),
            }
        }
        _ => Operator::new(Op::combine(), projections),
    };

    if function.contains_agg() || !groups.is_empty() {
        let group = match groups.len() {
            1 => Some(groups.pop().unwrap()),
            0 => None,
            _ => Some(Operator::combine(groups)),
        };

        root.aggregate(function, group);
        return Ok(root);
    }

    root.project(function);
    Ok(root)
}

fn handle_from(from: SqlStatement) -> Result<AlgebraRoot, String> {
    match from {
        Identifier(i) => handle_table(i),
        SqlStatement::Variable(v) => handle_variable(v),
        SqlStatement::Operator(o) => handle_collection_operator(o),
        SqlStatement::Select(s) => handle_select(s),
        err => Err(format!("Could not translate FROM clause: {:?}", err)),
    }
}

fn handle_collection_operator(operator: SqlOperator) -> Result<AlgebraRoot, String> {
    let op = operator.operator;
    let root: Option<AlgebraRoot> = operator
        .operands
        .into_iter()
        .map(handle_from)
        .map(|r| r.unwrap())
        .collect();

    let mut root = root.ok_or("Could not handle operator".to_string())?;

    root.alias(op.dump(false).to_lowercase());
    Ok(root)
}

fn handle_variable(variable: SqlVariable) -> Result<AlgebraRoot, String> {
    let root: Option<AlgebraRoot> = variable
        .inputs
        .into_iter()
        .map(|i| handle_from(i).unwrap())
        .collect();
    let mut root = root.ok_or("Could not handle variable".to_string())?;

    root.variable(variable.name.clone());
    root.alias(variable.name);
    Ok(root)
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
            let operators = o
                .operands
                .into_iter()
                .map(|op| handle_field(op, aliases).unwrap())
                .collect();
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
                } else {
                    Operator::index(
                        aliases.iter().position(|s| s == &name).unwrap(),
                        vec![Operator::input()],
                    )
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
            let operators = l
                .list
                .into_iter()
                .map(|op| handle_field(op, aliases).unwrap())
                .collect();
            Ok(Operator::new(Op::combine(), operators))
        }
        SqlStatement::Value(v) => Ok(Operator::literal(v.value)),
        err => Err(format!("Could not translate operator: {:?}", err)),
    }
}

fn handle_table(identifier: SqlIdentifier) -> Result<AlgebraRoot, String> {
    let mut names = identifier.names.clone();
    let mut root = match names.remove(0) {
        name if name.starts_with('$') => name
            .strip_prefix('$')
            .ok_or("Prefix not found".to_string())
            .and_then(|rest| {
                rest.parse::<usize>()
                    .map_err(|_| "Could not parse number".to_string())
            })
            .map(|num| AlgebraRoot::new_scan_index(num))?,
        name => AlgebraRoot::new_scan_name(name),
    };
    if !names.is_empty() {
        let field = handle_field(Identifier(identifier), &vec![])?;
        root.project(field);
        root.alias(names.last().unwrap().clone());
    }

    Ok(root)
}

enum MaybeAliasAlg {
    Aliased(AliasedAlg),
    Raw(Algebraic),
}

struct AliasedAlg {
    name: String,
    alg: Algebraic,
}

impl MaybeAliasAlg {
    fn alg(&self) -> Algebraic {
        match self {
            MaybeAliasAlg::Aliased(a) => a.alg.clone(),
            MaybeAliasAlg::Raw(r) => r.clone(),
        }
    }

    fn aliased(name: String, alg: Algebraic) -> MaybeAliasAlg {
        MaybeAliasAlg::Aliased(AliasedAlg { name, alg })
    }

    fn raw(alg: Algebraic) -> MaybeAliasAlg {
        MaybeAliasAlg::Raw(alg)
    }
}
