use crate::models::configuration::{ConfigModel, ContainerConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize)]
pub struct LineModel {
    pub num: usize,
    pub stops: Vec<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct StopModel {
    pub num: usize,
    pub transform: Option<ContainerConfig>,
    pub sources: Vec<SourceModel>,
    pub destinations: Vec<DestinationModel>,
}

#[derive(Serialize, Deserialize)]
pub struct SourceModel {
    pub type_name: String,
    pub id: String,
    pub configs: HashMap<String, ConfigModel>,
}

#[derive(Serialize, Deserialize)]
pub struct DestinationModel {
    pub type_name: String,
    pub id: String,
    pub configs: HashMap<String, ConfigModel>,
}

#[derive(Serialize, Deserialize)]
pub struct TransformModel {
    language: String,
    query: String,
}