use serde::Deserialize;
use std::collections::HashMap;
use util::definition::{DefinitionFilter, Model};
use util::{NativeMapping, Query};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub def: HashMap<String, DefinitionModel>,
}

#[derive(Debug, Deserialize)]
pub struct DefinitionModel {
    pub topic: String,
    pub model: Model,
    pub entity: String,
    pub filter: DefinitionFilter,
    pub mapping: NativeMapping,
    pub processing: Query,
}

#[cfg(test)]
mod tests {
    use super::*;
    use util::RelationalMapping;

    #[tokio::test]
    async fn relational() {
        let mapping = r#"relational = [{name = "TEXT"}, {age = "INT"}]"#;

        let mapping: NativeMapping = toml::from_str(&mapping).unwrap();
        if let NativeMapping::Relational(r) = mapping {
            if let RelationalMapping::Tuple(val, _m) = r {
                assert_eq!(val[0].0, "name");
                assert_eq!(val[1].0, "age");
            }
        } else {
            assert!(false);
        }
    }

    #[tokio::test]
    async fn long() {
        let mapping = r#"
        [def.relational-default]
        topic = "Relational test"
        model = "relational"
        entity = "relational"
        filter.topic = "relational"
        processing.sql = "None"
        mapping.relational = [
            {name = "TEXT"}, {age = "INT"}
        ]

        [def.document-default]
        topic = "Document test"
        model = "document"
        entity = "document"
        filter.topic = "doc"
        mapping.document = "document"
        processing.mql = "None"

        [def.graph-default]
        topic = "Graph test"
        model = "graph"
        entity = "graph"
        filter.topic = "graph"
        processing.cypher = "None"
        [def.graph-default.mapping.graph.node]
        id = {doc.key = "id"}
        label = {doc.key = "label"}
        properties = {doc.key = "properties"}
        "#;

        let config: Config = toml::from_str(&mapping).unwrap();
    }

    #[tokio::test]
    async fn document() {
        let mapping = r#"
        [def.document-default]
        topic = "Document test"
        model = "document"
        entity = "document"
        filter.topic = "doc"
        mapping.document = "document"
        processing.mql = "None""#;

        let config: Config = toml::from_str(&mapping).unwrap();
    }

    #[tokio::test]
    async fn graph() {
        let mapping = r#"
        [graph.node]
        id = {doc.key = "id"}
        label = {doc.key = "label"}
        properties = {doc.key = "properties"}
        processing = "None""#;

        let config: NativeMapping = toml::from_str(&mapping).unwrap();
    }

    #[tokio::test]
    async fn relational_full() {
        let mapping = r#"
        [def.relational-default]
        topic = "Relational test"
        model = "relational"
        entity = "relational"
        filter.topic = "relational"
        mapping.relational = [
            {name = "TEXT"}, {age = "INT"}
        ]
        processing = "None""#;

        let config: Config = toml::from_str(&mapping).unwrap();
    }
}
