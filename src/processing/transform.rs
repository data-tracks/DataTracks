use std::sync::Arc;

use crate::algebra::RefHandler;
use crate::language::Language;
use crate::processing::train::Train;
use crate::processing::transform::Transform::{Func, Lang};
use crate::value::{Dict, Value};
use crate::{algebra, language};

pub trait Taker: Send {
    fn take(&mut self, wagons: &mut Vec<Train>) -> Vec<Train>;
}

#[derive(Clone)]
pub enum Transform {
    Func(FuncTransform),
    Lang(LanguageTransform),
}


impl Default for Transform {
    fn default() -> Self {
        Func(FuncTransform::default())
    }
}


impl Transform {
    pub fn parse(stencil: String) -> Result<Transform, String> {
        match stencil.split_once('|') {
            None => Err("Wrong transform format.".to_string()),
            Some((module, logic)) => match Language::try_from(module) {
                Ok(lang) => Ok(Lang(LanguageTransform::parse(lang, logic))),
                Err(_) => Err("Wrong transform format.".to_string())
            },
        }
    }

    pub fn dump(&self) -> String {
        match self {
            Func(f) => f.dump(),
            Lang(f) => f.dump()
        }
    }

    pub fn apply(&self, stop: i64, wagons: Vec<Train>) -> Train {
        match self {
            Func(f) => (f.func)(stop, wagons),
            Lang(f) => f.func.process(stop, wagons)
        }
    }
}


pub struct LanguageTransform {
    pub(crate) language: Language,
    pub(crate) query: String,
    func: Box<dyn RefHandler + Send>,
}

impl Clone for LanguageTransform {
    fn clone(&self) -> Self {
        LanguageTransform { language: self.language.clone(), query: self.query.clone(), func: self.func.clone() }
    }
}

impl LanguageTransform {
    fn parse(language: Language, query: &str) -> LanguageTransform {
        let func = build_transformer(&language, query).unwrap();
        LanguageTransform { language, query: query.to_string(), func }
    }

    fn dump(&self) -> String {
        format!("{{{}|{}}}", self.language.name(), &self.query)
    }
}

fn build_transformer(language: &Language, query: &str) -> Result<Box<dyn RefHandler + Send + 'static>, String> {
    let algebra = match language {
        Language::Sql => language::sql::transform(query)?,
        Language::Mql => language::mql::transform(query)?
    };
    algebra::functionize(algebra)
}


#[derive(Clone)]
struct FuncValueHandler {
    func: fn(train: Value) -> Value,
}


impl RefHandler for FuncValueHandler {
    fn process(&self, stop: i64, wagons: Vec<Train>) -> Train {
        let mut values = vec![];
        for mut train in wagons {
            let mut vals = train.values.take().unwrap().into_iter().map(|v| (self.func)(v)).collect();
            values.append(&mut vals);
        }

        Train::new(stop, values)
    }

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static> {
        Box::new(FuncValueHandler { func: self.func })
    }
}

#[derive(Clone)]
pub struct FuncTransform {
    pub func: Arc<dyn Fn(i64, Vec<Train>) -> Train + Send + Sync>,
}

impl Default for FuncTransform {
    fn default() -> Self {
        Self::new(Arc::new(|_stop, trains| Train::from(trains)))
    }
}

impl FuncTransform {
    pub(crate) fn new_boxed(func: fn(i64, Vec<Train>) -> Train) -> Self {
        Self::new(Arc::new(func))
    }

    pub(crate) fn new(func: Arc<(dyn Fn(i64, Vec<Train>) -> Train + Send + Sync)>) -> Self {
        FuncTransform { func }
    }

    pub(crate) fn new_val(_stop: i64, func: fn(Value) -> Value) -> FuncTransform {
        Self::new(Arc::new(move |stop, wagons| {
            let mut values = vec![];
            for mut train in wagons {
                let mut vals = train.values.take().unwrap().into_iter().map(func).collect();
                values.append(&mut vals);
            }

            Train::new(stop, values)
        }))
    }

    fn dump(&self) -> String {
        "".to_string()
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crossbeam::channel::unbounded;

    use crate::processing::station::Station;
    use crate::processing::tests::dict_values;
    use crate::processing::train::Train;
    use crate::processing::transform::Dict;
    use crate::processing::transform::FuncTransform;
    use crate::processing::transform::Transform::Func;
    use crate::util::new_channel;
    use crate::value::Value;

    #[test]
    fn transform() {
        let mut station = Station::new(0);

        let control = unbounded();

        station.set_transform(Func(FuncTransform::new_val(0, |mut x| {
            x.as_dict().unwrap().0.insert("$".into(), x.as_dict().unwrap().get_data().unwrap() + &Value::int(3));
            x
        })));

        let values = dict_values(vec![Value::float(3.3), Value::int(3)]);

        let (tx, num, rx) = new_channel();

        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0));
        station.send(Train::new(0, values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(mut t) => {
                assert_eq!(values.len(), t.values.clone().map_or(usize::MAX, |vec| vec.len()));
                for (i, value) in t.values.take().unwrap().into_iter().enumerate() {
                    assert_eq!(value.as_dict().unwrap().get_data().unwrap().clone(), &values[i].as_dict().unwrap().get_data().unwrap().clone() + &Value::int(3));
                    assert_ne!(Value::Dict(Dict::from(Value::text(""))), value)
                }
            }
            Err(..) => assert!(false),
        }
    }

    #[test]
    fn sql_transform() {
        let mut station = Station::new(0);

        let control = unbounded();

        station.set_transform(Func(FuncTransform::new_val(0, |mut x| {
            x.as_dict().unwrap().0.insert("$".into(), x.as_dict().unwrap().get_data().unwrap() + &Value::int(3));
            x
        })));

        let values = vec![Value::float(3.3).into(), Value::int(3).into()];

        let (tx, num, rx) = new_channel();

        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0));
        station.send(Train::new(0, values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(mut t) => {
                if let Some(vec) = t.values.take() {
                    assert_eq!(values.len(), vec.len());
                    for (i, value) in vec.into_iter().enumerate() {
                        assert_eq!(value.as_dict().unwrap().get_data().unwrap().clone(), values.get(i).unwrap().get_data().unwrap() + &Value::int(3));
                        assert_ne!(&Value::text(""), value.as_dict().unwrap().get_data().unwrap());
                    }
                } else {
                    panic!("Expected values for key 0");
                }
            }
            Err(e) => panic!("Failed to receive: {:?}", e),
        }
    }
}