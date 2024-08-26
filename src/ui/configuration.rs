use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub enum ConfigModel {
    StringConf(StringModel),
    NumberConf(NumberModel),
    ListConf(ListModel),
}

#[derive(Serialize, Deserialize)]
pub struct ConfigContainer {
    name: String,
    configs: HashMap<String, ConfigModel>,
}

impl ConfigContainer {
    pub(crate) fn new(name: String, configs: HashMap<String, ConfigModel>) -> ConfigContainer {
        ConfigContainer { name, configs }
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct BaseConfig {}

#[derive(Serialize, Deserialize)]
pub struct StringModel {
    base_config: BaseConfig,
    string: String,
}

impl StringModel {
    pub(crate) fn new(string: &str) -> StringModel {
        StringModel { base_config: BaseConfig::default(), string: string.to_string() }
    }
}

#[derive(Serialize, Deserialize)]
pub struct NumberModel {
    base_config: BaseConfig,
    number: i64,
}

#[derive(Serialize, Deserialize)]
pub struct ListModel {
    addable: bool,
    base_config: BaseConfig,
    list: Vec<ConfigModel>,
}



