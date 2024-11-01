use crate::util::Segment::DynamicIndex;
use crate::util::StringBuilder;
use crate::value::{Array, Dict, Value};

/**
DynamicQueries can come in two forms, either they access values by keys, which is intended for
dictionaries or via index, which is intended for arrays. Additionally, both allow to access the full input
**/
pub struct DynamicQuery{
    parts: Vec<Segment>,
    estimated_size: usize,
    replace_type: ReplaceType
}


impl DynamicQuery{

    fn build_dynamic_query(query: String) -> Self {
        let mut parts = vec![];

        let mut temp = StringBuilder::new();
        let mut is_text = false;
        let mut is_dynamic = false;

        for char in query.chars() {
            if is_dynamic && char == ' ' {
                // we finish the value
                if temp.is_empty(){
                    parts.push(DynamicQuery::full());
                }else if let Ok(num) = temp.build_and_clear().parse::<usize>(){
                    parts.push(DynamicQuery::index(num));
                }else {
                    parts.push(DynamicQuery::text(temp.build_and_clear()));
                }
                is_dynamic = false
            }else if char == '"' {
                is_text = !is_text;
            }else if char == '$' && !is_text {
                if !temp.is_empty() {
                    parts.push(DynamicQuery::text(temp.build_and_clear()));
                }
                is_dynamic = true;
            }else {
                temp.append(char);
            }
        }

        DynamicQuery::new(parts)
    }


    pub fn new(parts: Vec<Segment>) -> DynamicQuery{
        let estimated_size = parts.iter().map(|p| {
            match p {
                Segment::Static(s) => s.len(),
                Segment::DynamicIndex(_) | Segment::DynamicKey(_) => 10,
                Segment::DynamicFull => 10
            }
        }).sum::<usize>();
        let replace_type = if parts.iter().all(|p| matches!(p, Segment::DynamicFull)){
            ReplaceType::Full
        }else if parts.iter().all(|p| matches!(p, Segment::DynamicKey(_)) ){
            ReplaceType::Key
        }else {
            ReplaceType::Index
        };
        DynamicQuery{parts, estimated_size, replace_type}
    }

    pub fn construct(&self, value: Value)-> String{
        match self.replace_type{
            ReplaceType::Key => {
                match value{
                    Value::Dict(d) => self.construct_key(d),
                    Value::Wagon(w) => self.construct(w.unwrap()),
                    _ => self.construct_full(value),
                }
            }
            ReplaceType::Index => {
                match value{
                    Value::Array(a) => self.construct_index(a),
                    Value::Wagon(w) => self.construct(w.unwrap()),
                    _ => self.construct_full(value)
                }
            }
            ReplaceType::Full => {
                self.construct_full(value)
            }
        }
    }

    fn construct_full(&self, value: Value)-> String{
        let mut query = String::with_capacity(self.estimated_size);

        for part in &self.parts {
            match part {
                Segment::Static(s) => query.push_str(s),
                Segment::DynamicFull => query.push_str(&value.to_string()),
                _ => unreachable!()
            }
        }
        query
    }

    fn construct_key(&self, dict: Dict) -> String {
        let mut query = String::with_capacity(self.estimated_size);

        for part in &self.parts {
            match part {
                Segment::Static(s) => query.push_str(s),
                DynamicIndex(_) => unreachable!(),
                Segment::DynamicKey(k) => query.push_str(dict.get(k).unwrap().to_string().as_str()),
                Segment::DynamicFull => query.push_str(&dict.to_string()),
            }
        }

        query
    }

    fn construct_index(&self, values: Array) -> String {
        let mut query = String::with_capacity(self.estimated_size);

        for part in &self.parts {
            match part {
                Segment::Static(s) => query.push_str(s),
                Segment::DynamicIndex(d) => query.push_str(values.0.get(*d).unwrap().to_string().as_str()),
                Segment::DynamicKey(_) => {}
                Segment::DynamicFull => query.push_str(&values.to_string()),
            }
        }
        query
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

enum ReplaceType {
    Key,
    Index,
    Full
}



#[derive(PartialOrd, PartialEq)]
pub enum Segment{
    Static(String),
    DynamicIndex(usize),
    DynamicKey(String),
    DynamicFull
}