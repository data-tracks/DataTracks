use crate::algebra::implement_func;
use crate::algebra::Operator;
use crate::language::mql::MqlStatement;
use mongodb::{Collection, Database};
use value::Value;

#[derive(Clone)]
pub struct MongoDynamicQuery {
    query: String,
    parsed: MqlStatement,
}

impl MongoDynamicQuery {
    pub(crate) fn as_fn(&self) -> Result<DynamicExecutor, String> {
        match &self.parsed {
            MqlStatement::Insert(i) => {

                let op = if let MqlStatement::Value(v) = *i.values.clone() {
                    Operator::literal(v.value.clone())
                } else if let MqlStatement::Dynamic(d) = *i.values.clone() {
                    let index = d.id.clone();
                    Operator::context(index)
                } else {
                    return Err("Unsupported value in dynamic query".to_string());
                };

                let op = implement_func(&op);

                Ok(DynamicExecutor::Insert(i.collection.clone(), Box::new( move |value: &Value| {
                    op.process(value)
                })))

            }
            MqlStatement::Delete(_) => {
                todo!()
            }
            MqlStatement::Update(_) => {
                todo!()
            }
            _ => todo!()
        }
    }

    pub(crate) fn get_parsed(&self) -> MqlStatement {
        self.parsed.clone()
    }

    pub(crate) fn get_query(&self) -> String {
        self.query.clone()
    }
    pub fn new<S: AsRef<str>>(query: S) -> Result<Self, String> {
        let parsed = MongoDynamicQuery::parse(query.as_ref())?;
        Ok(Self { query: query.as_ref().to_string(), parsed })
    }

    pub fn parse<S: AsRef<str>>(query: S) -> Result<MqlStatement, String> {
        let parse = crate::language::mql::parse(query.as_ref())?;
        Ok(parse)
    }
}

pub enum DynamicExecutor {
    Insert(String, Box<dyn Fn(&Value) -> Value + Send + Sync + 'static>),
}


impl DynamicExecutor {

    pub fn prepare(&self, database: Database) -> Result<Collection<Value>, String> {
        match self {
            DynamicExecutor::Insert(collection, _) => {
                Ok(database.collection(collection))
            }
        }
    }

    pub async fn execute(&self, value: Value, collection: &Collection<Value>) -> Result<(), String> {
        match self {
            DynamicExecutor::Insert(_, handler) => {
                let value = handler(&value);
                collection.insert_one(value).await.map_err(|e| e.to_string())?;
                Ok(())
            }
        }
    }
}


