#[derive(PartialEq, Debug, Clone)]
pub enum ValType {
    Integer,
    Float,
    Text,
    Bool,
    Array,
    Dict,
    Null,
    Any
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