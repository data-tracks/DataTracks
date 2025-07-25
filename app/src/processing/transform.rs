use crate::algebra::{AlgebraRoot, BoxedIterator, ValueIterator};
use crate::analyse::{InputDerivable, OutputDerivable, OutputDerivationStrategy};
use crate::language;
use crate::language::Language;
use crate::optimize::OptimizeStrategy;
use crate::processing::Layout;
use crate::processing::option::Configurable;
#[cfg(test)]
use crate::processing::tests::DummyDatabase;
#[cfg(test)]
use crate::processing::transform::Transform::DummyDB;
use crate::processing::transform::Transform::{Func, Lang, Postgres, SQLite};
use crate::sql::{PostgresTransformer, SqliteTransformer};
use crate::util::storage::ValueStore;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use serde_json::Map;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use track_rails::message_generated::protocol::{
    LanguageTransform as FlatLanguageTransform, Transform as FlatTransform,
};
use track_rails::message_generated::protocol::{
    LanguageTransformArgs, TransformArgs, TransformType,
};
use value::Value;

#[derive(Debug, PartialEq)]
pub enum Transform {
    Func(FuncTransform),
    Lang(LanguageTransform),
    SQLite(SqliteTransformer),
    Postgres(PostgresTransformer),
    #[cfg(test)]
    DummyDB(DummyDatabase),
}

impl Clone for Transform {
    fn clone(&self) -> Self {
        match self {
            Func(f) => Func(Clone::clone(f)),
            Lang(language) => Lang(language.clone()),
            SQLite(s) => SQLite(s.clone()),
            Postgres(p) => Postgres(p.clone()),
            #[cfg(test)]
            DummyDB(d) => DummyDB(d.clone()),
        }
    }
}

impl Default for Transform {
    fn default() -> Self {
        Func(FuncTransform::default())
    }
}

impl Transform {
    pub fn parse(stencil: &str) -> Result<Transform, String> {
        if !stencil.contains('|') {
            return parse_function(stencil);
        }
        match stencil.split_once('|') {
            None => Err("Wrong transform format.".to_string()),
            Some((module, query)) => match Language::try_from(module) {
                Ok(lang) => Ok(Lang(LanguageTransform::parse(lang, query))),
                Err(_) => Err("Wrong transform format.".to_string()),
            },
        }
    }

    pub fn derive_input_layout(&self) -> Option<Layout> {
        match self {
            Func(f) => f.derive_input_layout(),
            Lang(l) => l.derive_input_layout(),
            SQLite(t) => t.derive_input_layout(),
            Postgres(p) => p.derive_input_layout(),
            #[cfg(test)]
            Transform::DummyDB(_) => todo!(),
        }
    }

    pub fn derive_output_layout(&self, inputs: HashMap<String, Layout>) -> Option<Layout> {
        match self {
            Func(f) => f.derive_output_layout(),
            Lang(l) => l.derive_output_layout(inputs),
            SQLite(c) => c.derive_output_layout(inputs),
            Postgres(p) => p.derive_output_layout(inputs),
            #[cfg(test)]
            DummyDB(_) => todo!(),
        }
    }

    pub fn dump(&self, _include_ids: bool) -> String {
        match self {
            Func(f) => f.dump(),
            Lang(f) => f.dump(),
            SQLite(c) => c.dump(),
            Postgres(p) => p.dump(),
            #[cfg(test)]
            DummyDB(_) => "DummyDB".to_string(),
        }
    }

    pub fn get_name(&self) -> String {
        match self {
            Func(_) => "Func".to_string(),
            Lang(_) => "Lang".to_string(),
            SQLite(c) => c.name(),
            Postgres(p) => p.name(),
            #[cfg(test)]
            DummyDB(d) => d.name(),
        }
    }

    pub fn flatternize<'a>(
        &self,
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<FlatTransform<'a>> {
        match self {
            Lang(l) => l.flatternize(builder),
            _ => todo!(),
        }
    }

    pub fn optimize(
        &self,
        transforms: HashMap<String, Transform>,
        optimizer: Option<OptimizeStrategy>,
    ) -> BoxedIterator {
        match self {
            Func(f) => f.derive_iter(),
            Lang(f) => {
                let root = f.algebra.clone();
                let mut optimized = if let Some(mut strategy) = optimizer {
                    strategy.apply(root).unwrap()
                } else {
                    root
                };

                let mut initial = optimized.derive_iterator().unwrap();
                let iter = initial.enrich(transforms);
                if let Some(iter) = iter { iter } else { initial }
            }
            SQLite(c) => c.optimize(transforms),
            Postgres(p) => p.optimize(transforms),
            #[cfg(test)]
            DummyDB(d) => d.optimize(transforms),
        }
    }
}

fn parse_function(stencil: &str) -> Result<Transform, String> {
    let (name, options) = stencil
        .split_once('{')
        .ok_or("Invalid transform format.".to_string())?;
    let name = name.trim();
    let (options, _) = options
        .trim()
        .rsplit_once('}')
        .ok_or("Invalid transform format.".to_string())?;

    let options = serde_json::from_str::<serde_json::Value>(&format!("{{{}}}", options))
        .unwrap()
        .as_object()
        .ok_or(format!("Invalid options: {}", options))?
        .clone();

    match name.to_lowercase().as_str() {
        #[cfg(test)]
        "dummy" => Ok(Func(FuncTransform::new_boxed(|_stop, value| {
            &value + &Value::int(1)
        }))),
        #[cfg(test)]
        "dummydb" => Ok(DummyDB(DummyDatabase::parse(options)?)),
        "sqlite" => Ok(SQLite(SqliteTransformer::parse(options)?)),
        "postgres" | "postgresql" => Ok(Postgres(PostgresTransformer::parse(options)?)),
        fun => panic!("Unknown function {}", fun),
    }
}

impl Configurable for Transform {
    fn name(&self) -> String {
        match self {
            Func(_) => "Func".to_string(),
            Lang(l) => l.language.to_string(),
            SQLite(c) => c.name(),
            Postgres(p) => p.name(),
            #[cfg(test)]
            DummyDB(_) => todo!(),
        }
    }

    fn options(&self) -> Map<String, serde_json::Value> {
        match self {
            Func(_) => Map::new(),
            Lang(_) => Map::new(),
            SQLite(c) => c.options(),
            Postgres(p) => p.options(),
            #[cfg(test)]
            DummyDB(_) => todo!(),
        }
    }
}

pub trait Transformer: Clone + Sized + Configurable + InputDerivable + OutputDerivable {
    fn parse(options: Map<String, serde_json::Value>) -> Result<Self, String>;

    fn optimize(
        &self,
        transforms: HashMap<String, Transform>,
    ) -> Box<dyn ValueIterator<Item = Value> + Send>;

    fn get_output_derivation_strategy(&self) -> &OutputDerivationStrategy;
}

impl<T: Transformer> OutputDerivable for T {
    fn derive_output_layout(&self, inputs: HashMap<String, Layout>) -> Option<Layout> {
        self.get_output_derivation_strategy()
            .derive_output_layout(inputs)
    }
}

pub struct LanguageTransform {
    pub(crate) language: Language,
    pub(crate) query: String,
    algebra: AlgebraRoot,
}

impl Debug for LanguageTransform {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("{}|{}", self.language, self.query))
            .finish()
    }
}

impl PartialEq for LanguageTransform {
    fn eq(&self, other: &Self) -> bool {
        self.query == other.query && self.language == other.language
    }
}

impl Clone for LanguageTransform {
    fn clone(&self) -> Self {
        LanguageTransform {
            language: self.language.clone(),
            query: self.query.clone(),
            algebra: self.algebra.clone(),
        }
    }
}

impl LanguageTransform {
    pub fn flatternize<'a>(
        &self,
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<FlatTransform<'a>> {
        let language = builder.create_string(&self.language.to_string());
        let query = builder.create_string(&self.query.to_string());
        let name = builder.create_string("Language");
        let args = FlatLanguageTransform::create(
            builder,
            &LanguageTransformArgs {
                language: Some(language),
                query: Some(query),
            },
        )
        .as_union_value();
        FlatTransform::create(
            builder,
            &TransformArgs {
                name: Some(name),
                // Add fields as needed
                type_type: TransformType::LanguageTransform,
                type_: Some(args),
            },
        )
    }

    fn parse(language: Language, query: &str) -> LanguageTransform {
        let algebra = build_algebra(&language, query).unwrap();
        LanguageTransform {
            language,
            query: query.to_string(),
            algebra,
        }
    }

    pub(crate) fn derive_input_layout(&self) -> Option<Layout> {
        self.algebra.derive_input_layout()
    }

    pub(crate) fn derive_output_layout(&self, inputs: HashMap<String, Layout>) -> Option<Layout> {
        self.algebra.derive_output_layout(inputs)
    }

    fn dump(&self) -> String {
        format!("{{{}|{}}}", self.language.name(), &self.query)
    }
}

pub fn build_algebra(language: &Language, query: &str) -> Result<AlgebraRoot, String> {
    match language {
        Language::Sql => language::sql::transform(query),
        Language::Mql => language::mql::transform(query),
    }
}

pub struct FuncTransform {
    //pub input: BoxedIterator,
    pub func: Arc<dyn Fn(i64, Value) -> Value + Send + Sync>,
    pub in_layout: Layout,
    pub out_layout: Layout,
}

impl Debug for FuncTransform {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Func".to_string().as_str())
    }
}

impl PartialEq for FuncTransform {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl Clone for FuncTransform {
    fn clone(&self) -> Self {
        FuncTransform {
            func: self.func.clone(),
            in_layout: self.in_layout.clone(),
            out_layout: self.out_layout.clone(),
        }
    }
}

impl Default for FuncTransform {
    fn default() -> Self {
        Self::new(Arc::new(|_stop, value| value))
    }
}

impl FuncTransform {
    #[cfg(test)]
    pub(crate) fn new_boxed(func: fn(i64, Value) -> Value) -> Self {
        Self::new(Arc::new(func))
    }

    pub(crate) fn new(func: Arc<(dyn Fn(i64, Value) -> Value + Send + Sync)>) -> Self {
        Self::new_with_layout(func, Layout::default(), Layout::default())
    }

    pub(crate) fn new_with_layout(
        func: Arc<(dyn Fn(i64, Value) -> Value + Send + Sync)>,
        in_layout: Layout,
        out_layout: Layout,
    ) -> Self {
        FuncTransform {
            func,
            in_layout,
            out_layout,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_val(_stop: i64, func: fn(Value) -> Value) -> FuncTransform {
        Self::new(Arc::new(move |_stop, value| func(value)))
    }

    pub(crate) fn derive_iter(&self) -> Box<FuncIter> {
        Box::new(FuncIter::new(self.func.clone()))
    }

    pub(crate) fn derive_input_layout(&self) -> Option<Layout> {
        Some(self.in_layout.clone())
    }

    pub(crate) fn derive_output_layout(&self) -> Option<Layout> {
        Some(self.out_layout.clone())
    }

    fn dump(&self) -> String {
        "".to_string()
    }
}

pub struct FuncIter {
    pub input: BoxedIterator,
    pub func: Arc<dyn Fn(i64, Value) -> Value + Send + Sync>,
}

impl FuncIter {
    fn new(func: Arc<dyn Fn(i64, Value) -> Value + Send + Sync>) -> Self {
        let mut scan = AlgebraRoot::new_scan_index(0);
        let input = scan.derive_iterator().unwrap();

        FuncIter { input, func }
    }
}

impl Iterator for FuncIter {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(val) = self.input.next() {
            Some((self.func)(0, val))
        } else {
            None
        }
    }
}

impl ValueIterator for FuncIter {
    fn get_storages(&self) -> Vec<ValueStore> {
        self.input.get_storages()
    }

    fn clone(&self) -> BoxedIterator {
        Box::new(FuncIter {
            input: self.input.clone(),
            func: self.func.clone(),
        })
    }

    fn enrich(&mut self, transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
        let func = self.input.enrich(transforms);

        if let Some(func) = func {
            self.input = func;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::language::Language;
    use crate::processing::station::Station;
    use crate::processing::tests::dict_values;
    use crate::processing::transform::Transform::Func;
    use crate::processing::transform::{FuncTransform, build_algebra};
    use crate::util::new_channel;
    use crossbeam::channel::unbounded;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::vec;
    use value::train::Train;
    use value::{Dict, Value};

    #[test]
    fn transform() {
        let mut station = Station::new(0);

        let control = unbounded();

        station.set_transform(Func(FuncTransform::new_val(0, |x| {
            let mut dict = x.as_dict().unwrap();
            dict.insert(
                "$".into(),
                x.as_dict().unwrap().get_data().unwrap() + &Value::int(3),
            );
            Value::Dict(dict)
        })));

        let values = dict_values(vec![Value::float(3.3), Value::int(3)]);

        let (tx, rx) = new_channel("test", false);

        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0), HashMap::new());
        station.fake_receive(Train::new(values.clone()));

        let res = rx.recv();
        match res {
            Ok(t) => {
                assert_eq!(values.len(), t.values.len());
                for (i, value) in t.values.into_iter().enumerate() {
                    assert_eq!(
                        value.as_dict().unwrap().get_data().unwrap().clone(),
                        &values[i].as_dict().unwrap().get_data().unwrap().clone() + &Value::int(3)
                    );
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
            dict.insert(
                "$".into(),
                x.as_dict().unwrap().get_data().unwrap() + &Value::int(3),
            );
            Value::Dict(dict)
        })));

        let values = dict_values(vec![Value::float(3.3), Value::int(3)]);

        let (tx, rx) = new_channel("test", false);

        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0), HashMap::new());
        station.fake_receive(Train::new(values.clone()));

        let res = rx.recv();
        match res {
            Ok(t) => {
                assert_eq!(values.len(), t.values.len());
                for (i, value) in t.values.into_iter().enumerate() {
                    assert_eq!(
                        value.as_dict().unwrap().get_data().unwrap().clone(),
                        values
                            .get(i)
                            .unwrap()
                            .as_dict()
                            .unwrap()
                            .get_data()
                            .unwrap()
                            + &Value::int(3)
                    );
                    assert_ne!(
                        &Value::text(""),
                        value.as_dict().unwrap().get_data().unwrap()
                    );
                }
            }
            Err(e) => panic!("Failed to receive: {:?}", e),
        }
    }

    #[test]
    fn sql_basic() {
        check_sql_implement(
            "SELECT * FROM $0",
            vec![Value::float(3.3)],
            vec![Value::float(3.3)],
        );
    }

    #[test]
    fn sql_basic_named() {
        check_sql_implement(
            "SELECT $0 FROM $0",
            vec![Value::float(3.3)],
            vec![Value::float(3.3)],
        );
    }

    #[test]
    fn sql_basic_key() {
        check_sql_implement(
            "SELECT $0.age FROM $0",
            vec![Value::dict_from_kv("age", Value::float(3.3))],
            vec![Value::float(3.3)],
        );
    }

    #[test]
    fn sql_basic_filter_match() {
        check_sql_implement(
            "SELECT $0.age FROM $0 WHERE $0.age = 25",
            vec![Value::dict_from_kv("age", Value::int(25))],
            vec![Value::int(25)],
        );
    }

    #[test]
    fn sql_basic_filter_non_match() {
        check_sql_implement(
            "SELECT $0.age FROM $0 WHERE $0.age = 23",
            vec![Value::dict_from_kv("age", Value::int(25))],
            vec![],
        );
    }

    #[test]
    fn sql_add() {
        check_sql_implement(
            "SELECT $0 + 1 FROM $0",
            vec![Value::float(3.3)],
            vec![Value::float(4.3)],
        );
    }

    #[test]
    fn sql_add_multiple() {
        check_sql_implement(
            "SELECT $0 + 1 + 0.3 FROM $0",
            vec![Value::float(3.3)],
            vec![Value::float(4.6)],
        );
    }

    #[test]
    fn sql_add_key() {
        check_sql_implement(
            "SELECT $0.age + 1 + 0.3 FROM $0",
            vec![Value::dict_from_kv("age", Value::float(3.3))],
            vec![Value::float(4.6)],
        );
    }

    #[test]
    fn sql_join() {
        check_sql_implement_join(
            "SELECT $0 + $1 FROM $0, $1",
            vec![vec![Value::float(3.3)], vec![Value::float(3.4)]],
            vec![Value::float(6.7)],
        );
    }

    #[test]
    fn sql_count_single() {
        check_sql_implement(
            "SELECT COUNT(*) FROM $0",
            vec![Value::float(3.3)],
            vec![Value::int(1)],
        );
    }

    #[test]
    fn sql_count_name() {
        check_sql_implement(
            "SELECT COUNT($0.age) FROM $0",
            vec![Value::dict_from_kv("age", Value::float(3.3))],
            vec![Value::int(1)],
        );
    }

    #[test]
    fn sql_sum_name() {
        check_sql_implement(
            "SELECT SUM($0.age) FROM $0",
            vec![Value::dict_from_kv("age", Value::float(3.3))],
            vec![Value::float(3.3)],
        );
    }

    #[test]
    fn sql_avg_name() {
        check_sql_implement(
            "SELECT AVG($0.age) FROM $0",
            vec![
                Value::dict_from_kv("age", Value::float(3.3)),
                Value::dict_from_kv("age", Value::float(3.7)),
            ],
            vec![Value::float(3.5)],
        );
    }

    #[test]
    fn sql_group_single() {
        check_sql_implement_unordered(
            "SELECT $0 FROM $0 GROUP BY $0",
            vec![Value::float(3.3), Value::float(3.3), Value::float(3.1)],
            vec![Value::float(3.1), Value::float(3.3)],
        );
    }

    fn check_sql_implement_join(query: &str, inputs: Vec<Vec<Value>>, output: Vec<Value>) {
        let transform = build_algebra(&Language::Sql, query);
        let transform = transform.unwrap().derive_iterator();

        match transform {
            Ok(mut t) => {
                for (i, input) in inputs.into_iter().enumerate() {
                    t.get_storages()
                        .iter_mut()
                        .find(|s| s.index == i)
                        .unwrap()
                        .append(input)
                }

                let result = t.drain_to_train(0);
                assert_eq!(result.values, output);
            }
            Err(_) => panic!("Failed"),
        }
    }

    fn check_sql_implement(query: &str, input: Vec<Value>, output: Vec<Value>) {
        let transform = build_algebra(&Language::Sql, query);
        let transform = transform.unwrap().derive_iterator();
        match transform {
            Ok(mut t) => {
                t.get_storages().first().unwrap().append(input);

                let result = t.drain_to_train(0);
                assert_eq!(result.values, output);
            }
            Err(_) => panic!("Failed"),
        }
    }

    fn check_sql_implement_unordered(query: &str, input: Vec<Value>, output: Vec<Value>) {
        let transform = build_algebra(&Language::Sql, query);
        let transform = transform.unwrap().derive_iterator();

        match transform {
            Ok(mut t) => {
                t.get_storages().first().unwrap().append(input);

                let result = t.drain_to_train(0);
                let result = result.values;
                for result in &result {
                    assert!(output.contains(result))
                }
                for output in &output {
                    assert!(result.contains(output))
                }
                assert_eq!(output.len(), result.len());
            }
            Err(_) => panic!(),
        }
    }
}
