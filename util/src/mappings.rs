use serde::Serialize;
use value::edge::Edge;
use value::node::Node;
use value::Value;
use value::Value::{Array, Dict};

#[derive(Clone, Debug, Serialize)]
/// The type of objects that can be produced by the definition, always of some specific data model
pub enum DefinitionMapping {
    // can produce Nodes, or Edegs or Subgraphs
    Graph(Mapping<GraphMapping>),
    // can produce Documents
    Document(DocumentMapping),
    // can produce rows/tuples
    Relational(RelationalMapping),
    // can produce kv-pair
    KeyValue(KeyValueMapping),
}

#[derive(Clone, Debug, Serialize)]
pub enum GraphMapping {
    Node(NodeMapping),
    Edge(EdgeMapping),
    SubGraph(Box<GraphMapping>),
}

#[derive(Clone, Debug, Serialize)]
pub enum DocumentMapping {
    Document(Mapping<MappingSource>),
}

#[derive(Clone, Debug, Serialize)]
pub enum RelationalMapping {
    Tuple(Mapping<MappingSource>),
}

#[derive(Clone, Debug, Serialize)]
pub enum KeyValueMapping {
    KV(Mapping<MappingSource>),
}

impl DefinitionMapping {
    pub fn document() -> Self {
        DefinitionMapping::Document(DocumentMapping::Document(Mapping {
            initial: MappingSource::Document(DocumentSource::Whole),
            manual: vec![],
            auto: vec![],
        }))
    }

    pub fn doc_to_graph() -> Self {
        DefinitionMapping::Graph(Mapping {
            initial: GraphMapping::Node(NodeMapping {
                id: MappingSource::Document(DocumentSource::Key("id".to_string())),
                label: MappingSource::Document(DocumentSource::Key("label".to_string())),
                properties: MappingSource::Document(DocumentSource::Key("properties".to_string())),
            }),
            manual: vec![],
            auto: vec![],
        })
    }

    pub fn build(&self) -> Box<dyn Fn(Value) -> Value + 'static + Send + Sync> {
        let mut funcs: Vec<Box<dyn Fn(&Value) -> Option<Value> + Sync + Send>> = vec![];
        match self {
            // we build a node, edge or subgraph
            DefinitionMapping::Graph(g) => {
                funcs.push(Self::handle_graph_mapping(&g.initial))
            }
            // we build a document in the end
            DefinitionMapping::Document(d) => {
                let DocumentMapping::Document(m) = d;

                funcs.push(Self::handle_doc_mapping(&m.initial));
                funcs.append(
                    &mut m
                        .manual
                        .iter()
                        .map(|m| Self::handle_doc_mapping(&m))
                        .collect(),
                );
                funcs.append(
                    &mut m
                        .auto
                        .iter()
                        .map(|m| Self::handle_doc_mapping(&m))
                        .collect(),
                );
            }
            DefinitionMapping::Relational(r) => {
                todo!()
            }
            DefinitionMapping::KeyValue(kv) => {
                todo!()
            }
        }
        Box::new(move |value: Value| {
            for map in &funcs {
                if let Some(next) = map(&value) {
                    return next;
                }
            }
            return Value::null();
        })
    }

    fn handle_doc_mapping(m: &MappingSource) -> Box<dyn Fn(&Value) -> Option<Value> + Send + Sync> {
        match m {
            // we get the data as a document
            MappingSource::Document(d) => match d {
                DocumentSource::Key(k) => {
                    let key = k.clone();
                    Box::new(move |v: &Value| {
                        if let Dict(d) = v {
                            return Some(d.get(&key).cloned().unwrap_or_default());
                        }
                        return None;
                    })
                }
                DocumentSource::Whole => Box::new(|v: &Value| {
                    if let Dict(_) = v {
                        return Some(v.clone());
                    }
                    return None;
                }),
            },
            // we get the data as List
            MappingSource::List { keys } => {
                let keys = keys.clone();
                Box::new(move |v: &Value| {
                    if let Array(a) = v {
                        return Some(Value::dict_from_pairs(
                            keys.iter()
                                .map(|k| k.as_str())
                                .zip(a.values.clone())
                                .collect::<Vec<(&str, Value)>>(),
                        ));
                    }
                    return None;
                })
            }
        }
    }

    fn handle_graph_mapping(mapping: &GraphMapping) -> Box<dyn Fn(&Value) -> Option<Value> + Sync + Send> {
        match mapping {
            GraphMapping::Node(n) => {
                let id = Self::handle_doc_mapping(&n.id);
                let label = Self::handle_doc_mapping(&n.label);
                let properties = Self::handle_doc_mapping(&n.properties);

                Box::new(move |value: &Value| {
                    Some(Value::Node(Box::new(
                        Node {
                            id: id(&value).map(|i| i.as_int().ok().unwrap_or_default()).unwrap_or_default(),
                            labels: label(&value).map(|v| v.as_text().ok().map(|v| vec![v]).unwrap_or_default()).unwrap_or_default(),
                            properties: properties(&value).map(|v| v.as_dict().ok().map(|m| m.values).unwrap_or_default()).unwrap_or_default()
                        }
                    )))
                })
            }
            GraphMapping::Edge(e) => {
                let id = Self::handle_doc_mapping(&e.id);
                let label = Self::handle_doc_mapping(&e.label);
                let properties = Self::handle_doc_mapping(&e.properties);
                let start = Self::handle_doc_mapping(&e.properties);
                let end = Self::handle_doc_mapping(&e.properties);

                Box::new(move |value: &Value| {
                    Some(Value::Edge(Box::new(
                        Edge {
                            id: id(&value).unwrap_or_default(),
                            start: start(&value).map(|v|v.as_int()).map(|i| i.ok().map(|v| v.0).unwrap_or_default()).unwrap_or_default() as usize,
                            label: Some(label(&value).unwrap_or_default()),
                            properties: properties(&value).map(|v| v.as_dict().ok().map(|m| m.values).unwrap_or_default()).unwrap_or_default(),
                            end: end(&value).map(|v|v.as_int()).map(|i| i.ok().map(|v| v.0).unwrap_or_default()).unwrap_or_default() as usize,
                        }
                    )))
                })
            }
            GraphMapping::SubGraph(_) => {
                todo!()
            }
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Mapping<T> {
    initial: T,
    manual: Vec<T>,
    auto: Vec<T>,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeMapping {
    id: MappingSource,
    label: MappingSource,
    properties: MappingSource,
}

#[derive(Clone, Debug, Serialize)]
pub struct EdgeMapping {
    id: MappingSource,
    label: MappingSource,
    properties: MappingSource,
    source: MappingSource,
    target: MappingSource,
}

#[derive(Clone, Debug, Serialize)]
pub enum MappingSource {
    Document(DocumentSource),
    List { keys: Vec<String> },
}

impl MappingSource {
    fn document() -> Self {
        MappingSource::Document(DocumentSource::Whole)
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum DocumentSource {
    Key(String),
    Whole,
}
