use serde_json::{Map, Value};

pub trait Configurable {
    fn name(&self) -> String;

    fn dump(&self) -> String {
        format!(
            "{}{}",
            self.name(),
            serde_json::to_string(&self.options()).unwrap()
        )
    }
    fn options(&self) -> Map<String, Value>;
}
