use serde_json::{Map, Value};

pub trait Configurable {
    fn get_name(&self) -> String;

    fn dump(&self) -> String {
        format!("{}{}", self.get_name(), serde_json::to_string(&self.get_options()).unwrap())
    }
    fn get_options(&self) -> Map<String, Value>;
}