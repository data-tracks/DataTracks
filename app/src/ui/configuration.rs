use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};

#[derive(Serialize, Deserialize, Debug)]
pub enum ConfigModel {
    #[serde(rename = "StringConf")]
    String(StringModel),
    #[serde(rename = "NumberConf")]
    Number(NumberModel),
    #[serde(rename = "BoolConf")]
    Boolean(BooleanModel),
    #[serde(rename = "ListConf")]
    List(ListModel),
}

impl Display for ConfigModel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigModel::String(s) => std::fmt::Display::fmt(&s.string, f),
            ConfigModel::Number(n) => std::fmt::Display::fmt(&n.number, f),
            ConfigModel::Boolean(b) => std::fmt::Display::fmt(&b.boolean, f),
            ConfigModel::List(l) => l.list.fmt(f),
        }
    }
}

impl ConfigModel {
    pub fn text(text: &str) -> ConfigModel {
        ConfigModel::String(StringModel::new(text))
    }

    pub fn number(number: i64) -> ConfigModel {
        ConfigModel::Number(NumberModel::new(number))
    }

    pub fn as_str(&self) -> String {
        match self {
            ConfigModel::String(string) => string.string.clone(),
            ConfigModel::Number(num) => num.number.to_string(),
            ConfigModel::List(list) => list
                .list
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(","),
            ConfigModel::Boolean(b) => b.boolean.to_string(),
        }
    }

    pub fn as_int(&self) -> Result<usize, String> {
        match self {
            ConfigModel::String(string) => string
                .string
                .as_str()
                .parse::<usize>()
                .map_err(|e| e.to_string()),
            ConfigModel::Number(number) => Ok(number.number as usize),
            ConfigModel::List(_) => Err("Cannot transform list to number.".to_string()),
            ConfigModel::Boolean(i) => i
                .boolean
                .to_string()
                .as_str()
                .parse::<usize>()
                .map_err(|e| e.to_string()),
        }
    }
}

impl From<String> for ConfigModel {
    fn from(value: String) -> Self {
        ConfigModel::String(StringModel::new(&value))
    }
}

impl From<&str> for ConfigModel {
    fn from(value: &str) -> Self {
        ConfigModel::String(StringModel::new(value))
    }
}

impl From<i64> for ConfigModel {
    fn from(value: i64) -> Self {
        ConfigModel::Number(NumberModel::new(value))
    }
}

impl From<u16> for ConfigModel {
    fn from(value: u16) -> Self {
        ConfigModel::Number(NumberModel::new(value as i64))
    }
}

impl From<bool> for ConfigModel {
    fn from(value: bool) -> Self {
        ConfigModel::Boolean(BooleanModel::new(value))
    }
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
        StringModel {
            base_config: BaseConfig::default(),
            string: string.to_string(),
        }
    }
}

impl From<String> for StringModel {
    fn from(value: String) -> Self {
        StringModel::new(&value)
    }
}

impl From<&str> for StringModel {
    fn from(value: &str) -> Self {
        StringModel::new(value)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BooleanModel {
    base_config: BaseConfig,
    boolean: bool,
}

impl BooleanModel {
    pub(crate) fn new(boolean: bool) -> Self {
        BooleanModel {
            base_config: BaseConfig::default(),
            boolean,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NumberModel {
    base_config: BaseConfig,
    pub number: i64,
}

impl NumberModel {
    pub(crate) fn new(number: i64) -> NumberModel {
        NumberModel {
            base_config: BaseConfig::default(),
            number,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListModel {
    addable: bool,
    base_config: BaseConfig,
    list: Vec<ConfigModel>,
}
