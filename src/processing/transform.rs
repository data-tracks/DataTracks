use crate::{algebra, language};
use crate::language::Language;
use crate::processing::train::Train;
use crate::processing::transform::Transform::Func;
use crate::value::Value;

pub struct Transformer(pub Box<dyn Fn(Train) -> Train + Send + 'static>);

pub enum Transform {
    Func(FuncTransform),
    LanguageTransform(LanguageTransform),
}

impl Transform {
    pub fn transformer(&mut self) -> Transformer {
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
    fn get_transform(&mut self) -> Transformer {
        Transformer(Box::new(|train: Train| {
            return train;
        }))
    }

    fn dump(&self) -> String;

    fn default() -> FuncTransform {
        FuncTransform::new(|f| f)
    }
}

pub struct LanguageTransform {
    language: Language,
    query: String,
    func: Transformer,
}

impl LanguageTransform {
    fn parse(language: Language, query: &str) -> LanguageTransform {
        let func = build_transformer(&language, query).unwrap();
        LanguageTransform { language, query: query.to_string(), func }
    }
}

fn build_transformer(language: &Language, query: &str) -> Result<Transformer, String> {
    let algebra = match language {
        Language::SQL => language::sql(query)?,
        Language::MQL => Err("Not supported.")?
    };
    algebra::funtionize(algebra)
}

impl Transformable for LanguageTransform {
    fn get_transform(&mut self) -> Transformer {
        todo!()
    }

    fn dump(&self) -> String {
        "{".to_owned() + &self.language.name().clone() + "|" + &self.query.clone() + "}"
    }
}


pub struct FuncTransform {
    pub func: Option<Transformer>,
}


impl FuncTransform {
    pub(crate) fn new<F>(func: F) -> Self
    where
        F: Fn(Train) -> Train + Send + 'static,
    {
        FuncTransform { func: Some(Transformer(Box::new(func))) }
    }

    pub(crate) fn new_val<F>(func: F) -> FuncTransform
    where
        F: Fn(Value) -> Value + Send + Clone + 'static,
    {
        Self::new(move |t: Train| {
            let values = t.values.get(&0).unwrap().into_iter().map(|value: &Value| func.clone()(value.clone())).collect();
            Train::single(values)
        })
    }
}

impl Transformable for FuncTransform {
    fn get_transform(&mut self) -> Transformer {
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

        station.transform(Func(FuncTransform::new_val(|x| &x + &Value::int(3))));

        let values = vec![Value::float(3.3), Value::int(3)];

        let (tx, rx) = channel();

        station.add_out(0, tx).unwrap();
        station.operate();
        station.send(Train::single(values.clone())).unwrap();

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

        station.transform(Func(FuncTransform::new_val(|x| &x + &Value::int(3))));

        let values = vec![Value::float(3.3), Value::int(3)];

        let (tx, rx) = channel();

        station.add_out(0, tx).unwrap();
        station.operate();
        station.send(Train::single(values.clone())).unwrap();

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