use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConfigModel {
    #[serde(rename = "StringConf")]
    String(StringModel),
    #[serde(rename = "NumberConf")]
    Number(NumberModel),
    #[serde(rename = "ListConf")]
    List(ListModel),
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

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct BaseConfig {}

#[derive(Serialize, Deserialize, Debug)]
pub struct StringModel {
    base_config: BaseConfig,
    pub string: String,
}

impl StringModel {
    pub(crate) fn new(string: &str) -> StringModel {
        StringModel { base_config: BaseConfig::default(), string: string.to_string() }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NumberModel {
    base_config: BaseConfig,
    pub number: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListModel {
    addable: bool,
    base_config: BaseConfig,
    list: Vec<ConfigModel>,
}



