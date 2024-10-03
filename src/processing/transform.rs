use crate::algebra::{Algebra, BoxedIterator, Scan, ValueIterator};
use crate::language::Language;
use crate::processing::train::Train;
use crate::processing::transform::Transform::{Func, Lang};
use crate::value::Value;
use crate::{algebra, language};
use std::sync::Arc;

pub trait Taker: Send {
    fn take(&mut self, wagons: &mut Vec<Train>) -> Vec<Train>;
}

pub enum Transform {
    Func(FuncTransform),
    Lang(LanguageTransform),
}

impl Clone for Transform {
    fn clone(&self) -> Self {
        match self {
            Func(f) => {
                Func(Clone::clone(f))
            }
            Lang(language) => {
                Lang(language.clone())
            }
        }
    }
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

    pub fn optimize(&self) -> Box<dyn ValueIterator<Item=Value> + Send> {
        match self {
            Func(f) => ValueIterator::clone(f),
            Lang(f) => f.func.clone()
        }
    }
}


pub struct LanguageTransform {
    pub(crate) language: Language,
    pub(crate) query: String,
    func: BoxedIterator,
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

fn build_transformer(language: &Language, query: &str) -> Result<Box<dyn ValueIterator<Item=Value> + Send + 'static>, String> {
    let algebra = match language {
        Language::Sql => language::sql::transform(query)?,
        Language::Mql => language::mql::transform(query)?
    };
    algebra::build_iterator(algebra)
}


pub struct FuncTransform {
    pub input: BoxedIterator,
    pub func: Arc<dyn Fn(i64, Value) -> Value + Send + Sync>,
}

impl Clone for FuncTransform {
    fn clone(&self) -> Self {
        FuncTransform { input: self.input.clone(), func: self.func.clone() }
    }
}

impl Default for FuncTransform {
    fn default() -> Self {
        Self::new(Arc::new(|_stop, value| value))
    }
}

impl FuncTransform {
    pub(crate) fn new_boxed(func: fn(i64, Value) -> Value) -> Self {
        Self::new(Arc::new(func))
    }

    pub(crate) fn new(func: Arc<(dyn Fn(i64, Value) -> Value + Send + Sync)>) -> Self {
        let mut scan = Scan::new(0);
        let iterator = scan.derive_iterator();
        FuncTransform{ input: Box::new(iterator), func }
    }

    pub(crate) fn new_val(_stop: i64, func: fn(Value) -> Value) -> FuncTransform {
        Self::new(Arc::new(move |_stop, value| {
            func(value)
        }))
    }

    fn dump(&self) -> String {
        "".to_string()
    }
}

impl Iterator for FuncTransform {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(val) = self.input.next() {
            Some((self.func)(0, val))
        }else {
            None
        }
    }
}

impl ValueIterator for FuncTransform {
    fn load(&mut self, trains: Vec<Train>) {
        self.input.load(trains);
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(FuncTransform { input: self.input.clone(), func: self.func.clone() })
    }
}


#[cfg(test)]
mod tests {
    use crate::language::Language;
    use crate::processing::station::Station;
    use crate::processing::tests::dict_values;
    use crate::processing::train::Train;
    use crate::processing::transform::Transform::Func;
    use crate::processing::transform::{build_transformer, FuncTransform};
    use crate::util::new_channel;
    use crate::value::{Dict, Value};
    use crossbeam::channel::unbounded;
    use std::sync::Arc;
    use std::vec;

    #[test]
    fn transform() {
        let mut station = Station::new(0);

        let control = unbounded();

        station.set_transform(Func(FuncTransform::new_val(0, |x| {
            let mut dict = x.as_dict().unwrap();
            dict.0.insert("$".into(), x.as_dict().unwrap().get_data().unwrap() + &Value::int(3));
            Value::Dict(dict)
        })));

        let values = dict_values(vec![Value::float(3.3), Value::int(3)]);

        let (tx, _num, rx) = new_channel();

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

        station.set_transform(Func(FuncTransform::new_val(0, |x| {
            let mut dict = x.as_dict().unwrap();
            dict.0.insert("$".into(), x.as_dict().unwrap().get_data().unwrap() + &Value::int(3));
            Value::Dict(dict)
        })));

        let values = dict_values(vec![Value::float(3.3).into(), Value::int(3).into()]);

        let (tx, _num, rx) = new_channel();

        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0));
        station.send(Train::new(0, values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(mut t) => {
                if let Some(vec) = t.values.take() {
                    assert_eq!(values.len(), vec.len());
                    for (i, value) in vec.into_iter().enumerate() {
                        assert_eq!(value.as_dict().unwrap().get_data().unwrap().clone(), values.get(i).unwrap().as_dict().unwrap().get_data().unwrap() + &Value::int(3));
                        assert_ne!(&Value::text(""), value.as_dict().unwrap().get_data().unwrap());
                    }
                } else {
                    panic!("Expected values for key 0");
                }
            }
            Err(e) => panic!("Failed to receive: {:?}", e),
        }
    }


    #[test]
    fn sql_basic() {
        check_sql_implement("SELECT * FROM $0", vec![Value::float(3.3)], vec![Value::float(3.3)]);
    }

    #[test]
    fn sql_basic_named() {
        check_sql_implement("SELECT $0 FROM $0", vec![Value::float(3.3)], vec![Value::float(3.3)]);
    }

    #[test]
    fn sql_basic_key() {
        check_sql_implement("SELECT $0.age FROM $0", vec![Value::dict_from_kv("age", Value::float(3.3))], vec![Value::float(3.3)]);
    }

    #[test]
    fn sql_basic_filter_match() {
        check_sql_implement("SELECT $0.age FROM $0 WHERE $0.age = 25", vec![Value::dict_from_kv("age", Value::int(25))], vec![Value::int(25)]);
    }

    #[test]
    fn sql_basic_filter_non_match() {
        check_sql_implement("SELECT $0.age FROM $0 WHERE $0.age = 25", vec![Value::dict_from_kv("age", Value::int(25))], vec![Value::int(25)]);
    }

    #[test]
    fn sql_add() {
        check_sql_implement("SELECT $0 + 1 FROM $0", vec![Value::float(3.3)], vec![Value::float(4.3)]);
    }

    #[test]
    fn sql_add_multiple() {
        check_sql_implement("SELECT $0 + 1 + 0.3 FROM $0", vec![Value::float(3.3)], vec![Value::float(4.6)]);
    }

    #[test]
    fn sql_add_key() {
        check_sql_implement("SELECT $0.age + 1 + 0.3 FROM $0", vec![Value::dict_from_kv("age", Value::float(3.3))], vec![Value::float(4.6)]);
    }

    #[test]
    fn sql_join() {
        check_sql_implement_join("SELECT $0 + $1 FROM $0, $1", vec![vec![Value::float(3.3)], vec![Value::float(3.4)]], vec![Value::float(6.7)]);
    }

    #[test]
    fn sql_count_single() {
        check_sql_implement("SELECT COUNT(*) FROM $0", vec![Value::float(3.3)], vec![Value::int(1)]);
    }

    #[test]
    fn sql_count_name() {
        check_sql_implement("SELECT COUNT($0.age) FROM $0", vec![Value::dict_from_kv("age", Value::float(3.3))], vec![Value::int(1)]);
    }

    #[test]
    fn sql_sum_name() {
        check_sql_implement("SELECT SUM($0.age) FROM $0", vec![Value::dict_from_kv("age", Value::float(3.3))], vec![Value::float(3.3)]);
    }

    #[test]
    fn sql_avg_name() {
        check_sql_implement("SELECT AVG($0.age) FROM $0", vec![Value::dict_from_kv("age", Value::float(3.3)), Value::dict_from_kv("age", Value::float(3.7))], vec![Value::float(3.5)]);
    }

    #[test]
    fn sql_group_single() {
        check_sql_implement("SELECT COUNT($0) FROM $0 GROUP BY $0",
                            vec![Value::float(3.3), Value::float(3.3), Value::float(3.1)],
                            vec![Value::float(3.1), Value::float(3.3)]);
    }

    fn check_sql_implement_join(query: &str, inputs: Vec<Vec<Value>>, output: Vec<Value>) {
        let transform = build_transformer(&Language::Sql, query);

        match transform {
            Ok(mut t) => {
                for (i, input) in inputs.into_iter().enumerate() {
                    t.load(vec![Train::new(i as i64, input)]);
                }

                let result = t.drain_to_train(0);
                assert_eq!(result.values.unwrap(), output);
            }
            Err(_) => panic!(),
        }
    }

    fn check_sql_implement(query: &str, input: Vec<Value>, output: Vec<Value>) {
        let transform = build_transformer(&Language::Sql, query);
        match transform {
            Ok(mut t) => {
                t.load(input.into_iter().map(|v| Train::new(0, vec![v])).collect());
                let result = t.drain_to_train(0);
                assert_eq!(result.values.unwrap(), output);
            }
            Err(_) => panic!(),
        }
    }
}