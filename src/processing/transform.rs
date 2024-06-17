use std::collections::HashMap;
use crate::{algebra, language};
use crate::language::Language;
use crate::processing::train::{Train};
use crate::processing::transform::Transform::Func;
use crate::value::Value;

pub type Referencer<'a> = Box<dyn Fn(&'a mut Train) -> Train + Send + Sync + 'a>;

pub type Taker<'a> = Box<dyn Fn(Train) -> Train + Send + Sync + 'a>;


pub enum Transform<'a> {
    Func(FuncTransform<'a>),
    LanguageTransform(LanguageTransform<'a>),
}


impl<'a> Transform<'a> {
    pub fn transformer(&mut self) -> Referencer {
        match self {
            Func(t) => t.get_transform(),
            Transform::LanguageTransform(t) => t.get_transform()
        }
    }

    pub fn default() -> Self {
        Func(FuncTransform::default())
    }

    pub fn parse(stencil: String) -> Result<Transform<'a>, String> {
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
    fn get_transform(&mut self) -> Referencer {
        Box::new(|train: &mut Train| Train::from(train))
    }

    fn dump(&self) -> String;

    fn default<'a>() -> FuncTransform<'a> {
        FuncTransform::new(|f| Train::from(f))
    }
}

pub struct LanguageTransform<'a> {
    language: Language,
    query: String,
    func: Option<Referencer<'a>>,
}

impl<'a> LanguageTransform<'a> {
    fn parse(language: Language, query: &str) -> LanguageTransform {
        let func = build_transformer(&language, query).unwrap();
        LanguageTransform { language, query: query.to_string(), func: Some(func) }
    }
}

fn build_transformer<'a>(language: &'a Language, query: &'a str) -> Result<Referencer<'a>, String> {
    let algebra = match language {
        Language::SQL => language::sql(query)?,
        Language::MQL => Err("Not supported.")?
    };
    algebra::funtionize(algebra)
}

impl<'a> Transformable for LanguageTransform<'a> {
    fn get_transform(&mut self) -> Referencer {
        self.func.take().unwrap()
    }

    fn dump(&self) -> String {
        "{".to_owned() + &self.language.name().clone() + "|" + &self.query.clone() + "}"
    }
}


pub struct FuncTransform<'a> {
    pub func: Option<Referencer<'a>>,
}


impl<'a> FuncTransform<'a> {
    pub(crate) fn new<F>(func: F) -> Self
    where
        F: Fn(&mut Train) -> Train + Send + 'a + Sync,
    {
        FuncTransform { func: Some(Box::new(func)) }
    }

    pub(crate) fn new_val<F>(stop: i64, func: F) -> FuncTransform<'a>
    where
        F: Fn(Value) -> Value + Send + Clone + Sync + 'a,
    {
        Self::new(move |t: &mut Train| {
            let mut  values = HashMap::new();
            for (stop, value) in &mut t.values {
                values.insert(stop.clone(), value.take().unwrap().into_iter().map(func.clone()).collect());
            }
            Train::new(values)
        })
    }
}

impl<'a> Transformable for FuncTransform<'a> {
    fn get_transform(&mut self) -> Referencer {
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