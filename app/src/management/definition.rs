use std::collections::HashMap;
use axum::extract::Query;
use value::Value;

/// Defines into which final entity an incoming value(primitive to complex) is stored
/// and provides "instructions" on identifying, which parts it is.
pub struct Definition {
    filter: Filter,
    model: Model,
    /// final destination
    entity: Option<String>,
    /// which "key|index" is used to identify a new value
    uniqueness: Vec<String>,
    query: Option<String>,
}


/// incoming values are either accompanied by meta with name or wrapped in a document structure
/// and have a matching value for the key
enum Filter {
    MetaName(String),
    KeyName(String, String),
}


enum Model {
    Document,
    Relational,
    Graph
}