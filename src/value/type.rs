#[derive(PartialEq, Debug, Clone)]
pub enum ValType {
    Integer,
    Float,
    Text,
    Bool,
    Time,
    Date,
    Array,
    Dict,
    Null,
    Any,
    Tuple
}

impl ValType {
    pub(crate) fn dump(&self, _quote: &str) -> String {
        match self {
            ValType::Integer => "int".to_string(),
            ValType::Float => "float".to_string(),
            ValType::Text => "text".to_string(),
            ValType::Bool => "bool".to_string(),
            ValType::Time => "time".to_string(),
            ValType::Date => "date".to_string(),
            ValType::Array => "array".to_string(),
            ValType::Dict => "dict".to_string(),
            ValType::Null => "null".to_string(),
            ValType::Any => "any".to_string(),
            ValType::Tuple => "tuple".to_string(),
        }.to_uppercase()
    }
}

impl ValType {
    pub(crate) fn parse(stencil: &str) -> ValType {
        match stencil.to_lowercase().as_str() {
            "int" | "integer" | "i" => ValType::Integer,
            "float" | "f" => ValType::Float,
            "bool" | "boolean" | "b" => ValType::Bool,
            "text" | "string" | "s" => ValType::Text,
            _ => panic!("Could not parse the type of the value.")
        }
    }
}