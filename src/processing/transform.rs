use std::collections::HashMap;

use crate::{algebra, language};
use crate::algebra::RefHandler;
use crate::language::Language;
use crate::processing::train::Train;
use crate::processing::transform::Transform::Func;
use crate::value::Value;

pub type Referencer = fn(&mut Train) -> Train;

pub type Taker = fn(Train) -> Train;


pub enum Transform {
    Func(FuncTransform),
    LanguageTransform(LanguageTransform),
}


impl Transform {
    pub fn transformer(&mut self) -> Box<dyn RefHandler> {
        match self {
            Func(t) => t.get_transform(),
            Transform::LanguageTransform(t) => t.get_transform()
        }
    }

    pub fn default() -> Self {
        Func(FuncTransform::default())
    }

    pub fn parse(stencil: String) -> Result<Transform, String> {
        match stencil.split_once("|") {
            None => Err("Wrong transform format.".to_string()),
            Some((module, logic)) => {
                match Language::try_from(module) {
                    Ok(lang) => Ok(Transform::LanguageTransform(LanguageTransform::parse(lang, logic))),
                    Err(_) => Err("Wrong transform format.".to_string())
                }
            }
        }
    }

    pub fn dump(&self) -> String {
        match self {
            Func(f) => f.dump(),
            Transform::LanguageTransform(f) => f.dump()
        }
    }
}

pub trait Transformable {
    fn get_transform(&mut self) -> Box<dyn RefHandler> {
        Box::new( FuncTransformHandler{func: |train: &mut Train| Train::from(train)})
    }

    fn dump(&self) -> String;

    fn default() -> FuncTransform {
        FuncTransform::new(|f| Train::from(f))
    }
}

pub struct LanguageTransform {
    language: Language,
    query: String,
    func: Option<Box<dyn RefHandler>>,
}

impl LanguageTransform {
    fn parse(language: Language, query: &str) -> LanguageTransform {
        let func = build_transformer(&language, query).unwrap();
        LanguageTransform { language, query: query.to_string(), func: Some(func) }
    }
}

fn build_transformer(language: &Language, query: &str) -> Result<Box<dyn RefHandler>, String> {
    let algebra = match language {
        Language::SQL => language::sql(query)?,
        Language::MQL => Err("Not supported.")?
    };
    algebra::functionize(algebra)
}

impl Transformable for LanguageTransform {
    fn get_transform(&mut self) -> Box<dyn RefHandler> {
        self.func.take().unwrap()
    }

    fn dump(&self) -> String {
        "{".to_owned() + &self.language.name().clone() + "|" + &self.query.clone() + "}"
    }
}


struct FuncTransformHandler {
    func: fn(&mut Train) -> Train,
}

impl RefHandler for FuncTransformHandler {
    fn process(&self, train: &mut Train) -> Train {
        self.func(train)
    }
}

pub struct FuncTransform {
    pub func: Option<Box<dyn RefHandler>>,
}


impl FuncTransform {
    pub(crate) fn new(func: fn(&mut Train) -> Train) -> Self {
        FuncTransform { func: Some(Box::new(FuncTransformHandler { func })) }
    }

    pub(crate) fn new_val<F>(stop: i64, func: F) -> FuncTransform
    where
        F: Fn(Value) -> Value + Send + Clone + Sync + 'static,
    {
        Self::new(move |t: &mut Train| {
            let mut values = HashMap::new();
            for (stop, value) in &mut t.values {
                values.insert(stop.clone(), value.take().unwrap().into_iter().map(func.clone()).collect());
            }
            Train::new(values)
        })
    }
}

impl Transformable for FuncTransform {
    fn get_transform(&mut self) -> Box<dyn RefHandler> {
        self.func.take().unwrap()
    }

    fn dump(&self) -> String {
        "".to_string()
    }
}


#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;

    use crate::processing::station::Station;
    use crate::processing::train::Train;
    use crate::processing::transform::FuncTransform;
    use crate::processing::transform::Transform::Func;
    use crate::value::Value;

    #[test]
    fn transform() {
        let mut station = Station::new(0);

        station.transform(Func(FuncTransform::new_val(0, |x| &x + &Value::int(3))));

        let values = vec![Value::float(3.3), Value::int(3)];

        let (tx, rx) = channel();

        station.add_out(0, tx).unwrap();
        station.operate();
        station.send(Train::single(0, values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(t) => {
                assert_eq!(values.len(), t.values.get(&0).unwrap().len());
                for (i, value) in t.values.get(&0).unwrap().iter().enumerate() {
                    assert_eq!(*value, &values[i] + &Value::int(3));
                    assert_ne!(Value::text(""), *value)
                }
            }
            Err(..) => assert!(false),
        }
    }

    #[test]
    fn sql_transform() {
        let mut station = Station::new(0);

        station.transform(Func(FuncTransform::new_val(0, |x| &x + &Value::int(3))));

        let values = vec![Value::float(3.3), Value::int(3)];

        let (tx, rx) = channel();

        station.add_out(0, tx).unwrap();
        station.operate();
        station.send(Train::single(0, values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(t) => {
                assert_eq!(values.len(), t.values.get(&0).unwrap().len());
                for (i, value) in t.values.get(&0).unwrap().iter().enumerate() {
                    assert_eq!(*value, &values[i] + &Value::int(3));
                    assert_ne!(Value::text(""), *value)
                }
            }
            Err(..) => assert!(false),
        }
    }
}