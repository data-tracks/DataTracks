use crate::language::Language;
use crate::processing::train::Train;
use crate::processing::transform::Transform::Func;
use crate::util;
use crate::value::Value;

pub enum Transform {
    Func(FuncTransform),
    LanguageTransform(LanguageTransform),
}

impl Transform {
    pub fn transformer(&self) -> Box<dyn Fn(Train) -> Train + Send + 'static> {
        match self {
            Func(t) => t.get_transform(),
            Transform::LanguageTransform(t) => t.get_transform()
        }
    }

    pub fn default() -> Self {
        Func(FuncTransform::default())
    }

    pub fn parse(stencil: String) -> Result<Transform, util::Error> {
        match stencil.split_once("|") {
            None => Err(util::Error::invalid_format("Wrong transform format.")),
            Some((module, logic)) => {
                match Language::try_from(module) {
                    Ok(lang) => Ok(Transform::LanguageTransform(LanguageTransform::parse(lang, logic))),
                    Err(_) => Err(util::Error::invalid_format("Wrong transform format."))
                }
            }
        }
    }

    pub fn dump(&self) -> String{
        match self {
            Func(f) => f.dump(),
            Transform::LanguageTransform(f) => f.dump()
        }
    }
}

pub trait Transformable {
    fn get_transform(&self) -> Box<dyn Fn(Train) -> Train + Send + 'static>;

    fn dump(&self) -> String;

    fn default() -> FuncTransform {
        FuncTransform::new(|f| f)
    }
}

pub struct LanguageTransform {
    language: Language,
    query: String,
}

impl LanguageTransform {
    pub(crate) fn get_func(&self) -> Box<dyn Fn(Train) -> Train + Send + 'static> {
        todo!()
    }

    fn parse(language: Language, query: &str) -> LanguageTransform {
        LanguageTransform { language, query: query.to_string() }
    }
}

impl Transformable for LanguageTransform {
    fn get_transform(&self) -> Box<dyn Fn(Train) -> Train + Send + 'static> {
        self.get_func()
    }

    fn dump(&self) -> String {
        "{".to_owned() + &self.language.name().clone() + "|" + &self.query.clone() + "}"
    }
}


pub struct FuncTransform {
    pub func: Option<Box<dyn Fn(Train) -> Train + Send + 'static>>,
}


impl FuncTransform {
    pub(crate) fn new<F>(func: F) -> Self where F: Fn(Train) -> Train + Send + 'static {
        FuncTransform { func: Some(Box::new(func)) }
    }

    pub(crate) fn new_val<F>(func: F) -> FuncTransform where F: Fn(Value) -> Value + Send + Clone + 'static {
        Self::new(move |t: Train| {
            let values = t.values.into_iter().map(func.clone()).collect();
            Train::new(values)
        })
    }

    pub(crate) fn get_func(&self) -> Box<dyn Fn(Train) -> Train + Send + 'static> {
        todo!()
    }
}

impl Transformable for FuncTransform {
    fn get_transform(&self) -> Box<dyn Fn(Train) -> Train + Send + 'static> {
        self.get_func()
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
    fn transform_test() {
        let mut station = Station::new(0);

        station.transform(Func(FuncTransform::new_val(|x| &x + &Value::int(3))));

        let values = vec![Value::float(3.3), Value::int(3)];

        let (tx, rx) = channel();

        station.add_out(0, tx);
        station.operate();
        station.send(Train::new(values.clone()));

        let res = rx.recv();
        match res {
            Ok(t) => {
                assert_eq!(values.len(), t.values.len());
                for (i, value) in t.values.iter().enumerate() {
                    assert_eq!(*value, &values[i] + &Value::int(3));
                    assert_ne!(Value::text(""), *value)
                }
            }
            Err(..) => assert!(false),
        }
    }
}