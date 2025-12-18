use crate::value::Value;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use json::parse;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::collections::btree_map::{IntoIter, Keys, Values};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use track_rails::message_generated::protocol::{
    Document, DocumentArgs, KeyValue, KeyValueArgs, Text, TextArgs, Value as FlatValue,
};

#[derive(
    Eq, Clone, Debug, Default, Serialize, Deserialize, Ord, PartialOrd, Readable, Writable,
)]
pub struct Dict {
    values: BTreeMap<String, Value>,
    alternative: BTreeMap<String, String>, // "alternative_name" -> Value
}

impl Dict {
    pub fn new(values: BTreeMap<String, Value>) -> Self {
        Dict {
            values,
            alternative: BTreeMap::new(),
        }
    }

    pub(crate) fn flatternize<'bldr>(
        &self,
        builder: &mut FlatBufferBuilder<'bldr>,
    ) -> WIPOffset<Document<'bldr>> {
        let values = self
            .values
            .iter()
            .map(|(k, v)| {
                let key_type = FlatValue::Text;
                let key = builder.create_string(k);
                let values_type = v.get_flat_type();
                let key =
                    Some(Text::create(builder, &TextArgs { data: Some(key) }).as_union_value());
                let values = Some(v.flatternize(builder).as_union_value());

                KeyValue::create(
                    builder,
                    &KeyValueArgs {
                        key_type,
                        key,
                        values_type,
                        values,
                    },
                )
            })
            .collect::<Vec<_>>();

        let values = builder.create_vector(values.as_slice());
        Document::create(builder, &DocumentArgs { data: Some(values) })
    }

    pub fn prefix_all(&mut self, prefix: &str) {
        self.values.iter().for_each(|(name, _field)| {
            self.alternative
                .insert(format!("{prefix}{name}"), name.clone());
        });
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        match self.values.get(key) {
            None => match self.alternative.get(key) {
                None => None,
                Some(s) => self.values.get(s),
            },
            Some(s) => Some(s),
        }
    }

    pub fn get_data(&self) -> Option<&Value> {
        self.values.get("$")
    }

    pub fn insert(&mut self, key: String, value: Value) {
        self.values.insert(key, value);
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub(crate) fn append(&mut self, other: &mut Dict) {
        self.values.append(&mut other.values);
    }

    pub(crate) fn keys(&self) -> Keys<'_, String, Value> {
        self.values.keys()
    }

    pub fn values(&self) -> Values<'_, String, Value> {
        self.values.values()
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, String, Value> {
        self.values.iter()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn merge(&self, other: Dict) -> Dict {
        let mut map = BTreeMap::new();
        for (key, value) in self.clone() {
            map.insert(key, value);
        }
        for (key, value) in other.clone() {
            map.insert(key, value);
        }
        if self.values.contains_key("$") && other.values.contains_key("$") {
            map.insert(
                "$".into(),
                vec![
                    self.get_data().unwrap().clone(),
                    other.get_data().unwrap().clone(),
                ]
                .into(),
            );
        }
        Dict {
            values: map,
            alternative: Default::default(),
        }
    }
}

impl PartialEq for Dict {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl Hash for Dict {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.values.hash(state);
    }
}

impl IntoIterator for Dict {
    type Item = (String, Value);
    type IntoIter = IntoIter<String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl Display for Dict {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{}}}",
            self.values
                .iter()
                .map(|(k, v)| format!("{k}: {v}"))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl From<Value> for Dict {
    fn from(value: Value) -> Self {
        let mut map = BTreeMap::new();
        match value {
            Value::Dict(d) => {
                for (key, value) in d.values {
                    map.insert(key, value);
                }
            }
            i => {
                map.insert("$".into(), i);
            }
        }
        Dict {
            values: map,
            alternative: BTreeMap::new(),
        }
    }
}

impl From<Vec<Value>> for Dict {
    fn from(value: Vec<Value>) -> Self {
        let mut map = BTreeMap::new();
        map.insert("$".into(), value.into());
        Dict {
            values: map,
            alternative: BTreeMap::new(),
        }
    }
}

impl Dict {
    pub fn from_json(value: &str) -> Self {
        let mut map = BTreeMap::new();
        for (key, value) in parse(value).unwrap().entries() {
            map.insert(key.into(), value.into());
        }
        Dict {
            values: map,
            alternative: BTreeMap::new(),
        }
    }
}