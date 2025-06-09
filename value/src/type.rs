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
    Tuple,
}

impl ValType {
    pub fn dump(&self, _quote: &str) -> String {
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
        }
        .to_uppercase()
    }
}

impl ValType {
    pub fn parse(stencil: &str) -> Result<ValType, String> {
        match stencil.to_lowercase().as_str() {
            "int" | "integer" | "i" => Ok(ValType::Integer),
            "float" | "f" => Ok(ValType::Float),
            "bool" | "boolean" | "b" => Ok(ValType::Bool),
            "text" | "string" | "s" => Ok(ValType::Text),
            _ => Err(String::from("Could not parse the type of the value.")),
        }
    }
}
