
use crate::queue::Meta;
use value::Value;
use value::Value::Dict;
use crate::definition::DefinitionFilter::AllMatch;
use crate::extractor::ValueExtractor;

/// Defines into which final entity an incoming value(primitive to complex) is stored
/// and provides "instructions" on identifying, which parts it is.
#[derive(Clone)]
pub struct Definition {
    filter: DefinitionFilter,
    pub model: Model,
    /// final destination
    pub entity: String,
    /// which "key|index" is used to identify a new value
    uniqueness: Vec<String>,
    query: Option<String>,
    ordering: Option<ValueExtractor>
}

impl Definition {
    pub fn new(filter: DefinitionFilter, model: Model, entity: String) -> Self {
        Definition {
            filter,
            model,
            entity,
            uniqueness: vec![],
            query: None,
            ordering: None,
        }
    }

    pub fn empty() -> Definition {
        Definition {
            filter: AllMatch,
            model: Model::Document,
            entity: String::from("_stream"),
            uniqueness: vec![],
            query: None,
            ordering: None,
        }
    }

    /// does our event match the defined definition
    pub fn matches(&mut self, value: &Value, meta: &Meta) -> bool {
        match &self.filter {
            AllMatch => true,
            DefinitionFilter::MetaName(n) => meta.name == Some(n.clone()),
            DefinitionFilter::KeyName(k, v) => match value {
                Dict(d) => d.get(&k) == Some(&Value::from(v.clone())),
                _ => false,
            },
        }
    }
}

/// incoming values are either accompanied by meta with name or wrapped in a document structure
/// and have a matching value for the key
#[derive(Clone)]
pub enum DefinitionFilter {
    AllMatch,
    MetaName(String),
    KeyName(String, String),
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum Model {
    Document,
    Relational,
    Graph,
}

