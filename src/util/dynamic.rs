use crate::util::StringBuilder;
use crate::value::Value;
use crate::value::Value::{Array, Dict};

/**
DynamicQueries can come in two forms, either they access values by keys, which is intended for
dictionaries or via index, which is intended for arrays. Additionally, both allow to access the full input
**/

#[derive(Clone, Debug, PartialEq)]
pub struct DynamicQuery {
    query: String,
    parts: Vec<Segment>,
    estimated_size: usize,
    replace_type: ReplaceType,
}


impl DynamicQuery {
    pub fn build_dynamic_query(query: String) -> Self {
        let mut parts = vec![];

        let mut temp = StringBuilder::new();
        let mut is_text = false;
        let mut is_dynamic = false;

        for char in query.chars() {
            if is_dynamic && char.is_whitespace() {
                // we finish the value
                if temp.is_empty() {
                    parts.push(DynamicQuery::full());
                } else if let Ok(num) = temp.build_and_clear().parse::<usize>() {
                    parts.push(DynamicQuery::index(num));
                } else {
                    parts.push(DynamicQuery::text(temp.build_and_clear()));
                }
                is_dynamic = false
            } else if char == '"' {
                is_text = !is_text;
            } else if char == '$' && !is_text {
                if !temp.is_empty() {
                    parts.push(DynamicQuery::text(temp.build_and_clear()));
                }
                is_dynamic = true;
            } else {
                temp.append(char);
            }
        }

        DynamicQuery::new(query, parts)
    }

    pub(crate) fn get_parts(&self) -> Vec<Segment> {
        self.parts.clone()
    }

    pub(crate) fn replace_indexed_query(&self, prefix: &str, placeholder: Option<&str>) -> String {
        let mut builder = StringBuilder::new();
        let mut i = 0;
        for part in &self.parts {
            match part {
                Segment::Static(s) => builder.append_string(s),
                Segment::DynamicIndex(_) | Segment::DynamicKey(_) | Segment::DynamicFull => {
                    let index = match placeholder {
                        None => i.to_string().as_str(),
                        Some(placeholder) => placeholder
                    };
                    let key = format!("{}{}", prefix, index);
                    builder.append_string(&key);
                    i += 1;
                }
            }
        }

        builder.build()
    }

    pub fn new(query: String, parts: Vec<Segment>) -> DynamicQuery {
        let estimated_size = parts.iter().map(|p| {
            match p {
                Segment::Static(s) => s.len(),
                Segment::DynamicIndex(_) | Segment::DynamicKey(_) => 10,
                Segment::DynamicFull => 10
            }
        }).sum::<usize>();
        let replace_type = if parts.iter().all(|p| matches!(p, Segment::DynamicFull)) {
            ReplaceType::Full
        } else if parts.iter().all(|p| matches!(p, Segment::DynamicKey(_))) {
            ReplaceType::Key
        } else {
            ReplaceType::Index
        };
        DynamicQuery { query, parts, estimated_size, replace_type }
    }

    pub fn get_replacement_type(&self) -> &ReplaceType {
        &self.replace_type
    }

    pub fn get_query(&self) -> String {
        self.query.clone()
    }

    pub fn prepare_query(&self, prefix: &str, placeholder: Option<&str>) -> (String, Box<dyn Fn(&Value) -> Vec<Value>>) {
        let query = self.replace_indexed_query(prefix, placeholder);
        let parts = self.parts.iter().filter(|p| !matches!(p, Segment::Static(_))).cloned().collect::<Vec<Segment>>();
        let parts: Vec<Box<dyn Fn(&Value) -> Value>> = parts.into_iter().map(|part| {
            let func: Box<dyn Fn(&Value) -> Value> = match part {
                Segment::DynamicIndex(i) => Box::new(move |value| {
                    if let Array(array) = value {
                        array.0.get(i).unwrap().clone()
                    } else {
                        panic!()
                    }
                }),
                Segment::DynamicKey(k) => Box::new(move |value| {
                    if let Dict(dict) = value {
                        dict.get(&k).unwrap().clone()
                    } else {
                        panic!()
                    }
                }),
                Segment::DynamicFull => Box::new(|value| {
                    value.clone()
                }),
                _ => unreachable!()
            };
            func
        }).collect();

        (query, Box::new(move |value| {
            parts.iter().map(|part| {
                let mut value = value.clone();
                while let Value::Wagon(w) = value {
                    value = w.clone().unwrap();
                }
                part(&value)
            }).collect()
        }))
    }

    fn text(text: String) -> Segment {
        Segment::Static(text)
    }

    fn index(index: usize) -> Segment {
        Segment::DynamicIndex(index)
    }

    fn key(key: String) -> Segment {
        Segment::DynamicKey(key)
    }

    fn full() -> Segment {
        Segment::DynamicFull
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReplaceType {
    Key,
    Index,
    Full,
}


#[derive(PartialOrd, PartialEq, Clone, Debug)]
pub enum Segment {
    Static(String),
    DynamicIndex(usize),
    DynamicKey(String),
    DynamicFull,
}