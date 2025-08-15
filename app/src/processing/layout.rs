use crate::algebra::order::Order;
use crate::processing::layout::OutputType::{Any, Array, Boolean, Dict, Float, Integer, Text};
use crate::processing::plan::PlanStage;
use crate::processing::OutputType::Tuple;
use crate::processing::Train;
use crate::util::BufferedReader;
use std::cmp::min;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::iter::zip;
use tracing::warn;
use value::{ValType, Value};
use OutputType::{And, Or};

const ARRAY_OPEN: char = '[';
const ARRAY_CLOSE: char = ']';
const DICT_OPEN: char = '{';
const DICT_CLOSE: char = '}';

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct Layout {
    pub name: Option<String>,
    pub explicit: bool,
    pub nullable: bool,
    pub optional: bool,
    pub type_: OutputType,
    pub order: Order,
}

impl Default for Layout {
    fn default() -> Self {
        Layout {
            name: None,
            explicit: false,
            nullable: false,
            optional: false,
            type_: Any,
            order: Order::None,
        }
    }
}

impl Layout {
    pub fn fits_train(&self, train: &Train) -> bool {
        train.clone().into_values().iter().all(|value| self.fits(value))
    }

    pub(crate) fn accepts(&self, other: &Layout) -> Result<(), String> {
        if !self.nullable && other.nullable {
            return Err("Input is nullable, but station is not".to_string());
        }
        if !self.optional && other.optional {
            return Err("Input is optional, but station is not".to_string());
        }

        self.type_.accepts(&other.type_)
    }

    pub(crate) fn merge(&self, other: &Layout) -> Layout {
        let mut layout = self.clone();
        layout.explicit = other.explicit || self.explicit;

        layout.nullable = other.nullable || self.nullable;

        layout.name = self.name.as_ref().or(other.name.as_ref()).cloned();

        match (&self.type_, &other.type_) {
            (Dict(this), Dict(other)) => {
                let mut dict = vec![];

                this.fields.iter().for_each(|f| dict.push(f.clone()));
                other
                    .fields
                    .iter()
                    .filter(|f| f.name.as_ref().is_none_or(|name| !this.contains_key(name)))
                    .for_each(|f| dict.push(f.clone()));
                layout.type_ = Dict(Box::new(DictType::new(dict)))
            }
            (Array(this), Array(other)) => {
                let mut array = ArrayType::new(this.fields.merge(&other.fields), None);

                array.length = match (this.length, other.length) {
                    (Some(this), Some(other)) => Some(min(this, other)),
                    (_, _) => None,
                }
            }
            (this, Any) | (Any, this) => {
                layout.type_ = this.clone();
            }
            (Text, _) | (_, Text) => {
                layout.type_ = Text;
            }
            (Integer, Float) => {
                layout.type_ = Integer;
            }
            (this, other) => {
                warn!("not yet handled if {:?} accepts {:?}", this, other);
                panic!()
                //layout.type_ = Or(vec![this.clone_boxed(), other.clone_boxed()]);
            }
        }
        layout
    }

    pub(crate) fn parse(stencil: &str) -> Layout {
        let mut reader = BufferedReader::new(stencil.to_string());

        parse(&mut reader)
    }

    pub fn dict(keys: Vec<String>) -> Layout {
        let mut layout = Layout::default();
        let mut dict = Vec::new();
        keys.into_iter().for_each(|v| {
            dict.push(Layout::from(v.as_str()));
        });
        layout.type_ = Dict(Box::new(DictType::new(dict)));
        layout
    }

    pub fn array(index: Option<i32>) -> Layout {
        Layout {
            type_: Array(Box::new(ArrayType::new(Layout::default(), index))),
            ..Default::default()
        }
    }

    pub fn tuple(names: Vec<Option<String>>) -> Layout {
        Layout {
            type_: Tuple(Box::new(TupleType::from(
                names
                    .into_iter()
                    .map(|n| n.map_or(Layout::default(), |n| Layout::from(n.as_str())))
                    .collect::<Vec<_>>(),
            ))),
            ..Default::default()
        }
    }

    pub fn or(left: OutputType, right: OutputType) -> Layout {
        Layout {
            name: None,
            explicit: false,
            nullable: false,
            optional: false,
            type_: Or(vec![left, right]),
            order: Default::default(),
        }
    }

    pub(crate) fn fits(&self, value: &Value) -> bool {
        self.type_.fits(value)
    }
}

impl From<OutputType> for Layout {
    fn from(type_: OutputType) -> Self {
        Layout {
            type_,
            ..Default::default()
        }
    }
}

impl From<(&str, Layout)> for Layout {
    fn from((name, mut type_): (&str, Layout)) -> Self {
        type_.name = Some(name.to_string());
        type_
    }
}

impl From<&str> for Layout {
    fn from(name: &str) -> Self {
        Layout {
            name: Some(name.to_string()),
            ..Default::default()
        }
    }
}

impl From<(&str, OutputType)> for Layout {
    fn from((name, type_): (&str, OutputType)) -> Self {
        Layout {
            name: Some(name.to_string()),
            type_,
            ..Default::default()
        }
    }
}

fn parse(reader: &mut BufferedReader) -> Layout {
    reader.consume_spaces();

    if let Some(char) = reader.next() {
        match char {
            DICT_OPEN => parse_dict(reader).0,
            ARRAY_OPEN => parse_array(reader).0,
            c => parse_type(reader, c).0,
        }
    } else {
        Layout::default()
    }
}

fn parse_dict_fields(reader: &mut BufferedReader) -> DictType {
    let mut builder = DictBuilder::default();

    reader.consume_if_next(DICT_OPEN);

    while let Some(char) = reader.next() {
        match char {
            ':' => {
                reader.consume_spaces();
                let type_char = reader.next().unwrap();
                let (layout, length) = parse_type(reader, type_char);
                if length.is_some() {
                    panic!("Dictionary already contains type");
                }
                builder.push_value(layout);
            }
            ',' => {
                reader.consume_spaces();
            }
            DICT_CLOSE => break,
            c => builder.push_key(c),
        }
    }

    reader.consume_if_next(DICT_CLOSE);

    DictType {
        fields: builder.build_fields(),
    }
}

#[derive(Default)]
pub struct DictBuilder {
    fields: Vec<Layout>,
    key: String,
}

impl DictBuilder {
    pub fn push_key(&mut self, char: char) {
        self.key.push(char)
    }

    pub fn push_value(&mut self, mut layout: Layout) {
        layout.name = Some(self.key.clone());
        self.fields.push(layout);
        self.key.clear()
    }

    pub fn build_fields(&mut self) -> Vec<Layout> {
        let fields = self.fields.clone();
        self.fields.clear();
        fields
    }
}

fn parse_type(reader: &mut BufferedReader, c: char) -> (Layout, Option<i32>) {
    match c {
        'i' => parse_field(Integer, reader),
        'f' => parse_field(Float, reader),
        't' => parse_field(Text, reader),
        'b' => parse_field(Boolean, reader),
        ARRAY_OPEN => parse_array(reader),
        DICT_OPEN => parse_dict(reader),
        prefix => panic!("Unknown output prefix: {prefix}"),
    }
}

fn parse_dict(reader: &mut BufferedReader) -> (Layout, Option<i32>) {
    let (mut field, length) = parse_field(Any, reader);
    reader.consume_spaces();

    field.type_ = Dict(Box::new(parse_dict_fields(reader)));
    (field, length)
}

fn parse_array(reader: &mut BufferedReader) -> (Layout, Option<i32>) {
    reader.consume_if_next(ARRAY_OPEN);
    reader.consume_spaces();
    let fields = if let Some(char) = reader.peek_next() {
        match char {
            ARRAY_CLOSE => (Layout::default(), None),
            c => {
                reader.next();
                parse_type(reader, c)
            }
        }
    } else {
        (Layout::default(), None)
    };

    reader.consume_if_next(ARRAY_CLOSE);

    let (mut field, length) = parse_field(Any, reader);

    field.type_ = Array(Box::new(ArrayType {
        fields: fields.0,
        length: fields.1,
    }));

    (field, length)
}

fn parse_field(type_: OutputType, reader: &mut BufferedReader) -> (Layout, Option<i32>) {
    // field : length
    let mut nullable = false;
    let mut optional = false;

    reader.consume_if_next(PlanStage::LAYOUT_OPEN);

    let mut length = None;

    while let Some(char) = reader.peek_next() {
        match char {
            ' ' => {}
            '?' => nullable = true,
            '\'' => optional = true,
            ':' => {
                let mut num = String::new();
                while let Some(char) = reader.peek_next() {
                    if char.is_ascii_digit() {
                        num.push(char);
                        reader.next();
                    } else {
                        if !num.is_empty() {
                            length = Some(num.parse::<i32>().unwrap());
                        }
                        break;
                    }
                }
            }
            _ => {
                break;
            }
        };
        reader.next();
    }

    reader.consume_if_next(PlanStage::LAYOUT_CLOSE);

    (
        Layout {
            name: None,
            explicit: true,
            nullable,
            optional,
            type_,
            order: Default::default(),
        },
        length,
    )
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub enum OutputType {
    // single value
    Integer,
    Float,
    Text,
    Boolean,
    Time,
    Date,
    Any,
    Array(Box<ArrayType>), // variable length same type
    Tuple(Box<TupleType>), // fixed length different type
    Dict(Box<DictType>),
    And(Vec<OutputType>),
    Or(Vec<OutputType>),
}

impl OutputType {
    pub(crate) fn fits(&self, value: &Value) -> bool {
        match self {
            Any => true,
            Array(a) => match value {
                Value::Array(array) => a.fits(array),
                _ => false,
            },
            Dict(d) => match value {
                Value::Dict(dict) => d.fits(&Value::Dict(dict.clone())),
                _ => false,
            },
            t => value.type_() == t.value_type(),
        }
    }

    pub(crate) fn accepts(&self, other: &OutputType) -> Result<(), String> {
        match self {
            Integer | Float | Text | Boolean | OutputType::Time | OutputType::Date => match other {
                OutputType::Time | Integer | Float | Text | Boolean | OutputType::Date => Ok(()),
                Any | Array(_) | Dict(_) | Tuple(_) => self.type_mismatch_error(other),
                And(a) => {
                    if a.iter().all(|v| self.accepts(v).is_ok()) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
                Or(o) => {
                    if o.iter().any(|v| self.accepts(v).is_ok()) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
            },
            Any => Ok(()),
            Array(a) => match other {
                Array(o) => {
                    a.fields.accepts(&o.fields)?;
                    if a.length.is_some() && o.length.is_none() {
                        return self.type_mismatch_error(other);
                    }
                    if a.length.is_none() && o.length.is_none() {
                        return self.type_mismatch_error(other);
                    }
                    if a.length.unwrap() <= o.length.unwrap() {
                        return Err(format!(
                            "Type mismatch {:?} with length {} cannot accept {:?} with length {}",
                            a,
                            a.length.unwrap(),
                            o,
                            o.length.unwrap()
                        ));
                    }
                    Ok(())
                }
                Tuple(t) => {
                    if let Some(length) = a.length {
                        if length > t.fields.len() as i32 {
                            return Err(format!(
                                "Type mismatch {:?} with length {} cannot accept {:?} with length {}",
                                a,
                                length,
                                t,
                                t.fields.len()
                            ));
                        }
                    }

                    if t.fields.iter().all(|t| a.fields.accepts(t).is_ok()) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
                And(a) => {
                    if a.iter().all(|v| self.accepts(v).is_ok()) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
                Or(o) => {
                    if o.iter().any(|v| self.accepts(v).is_ok()) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
                _ => self.type_mismatch_error(other),
            },
            Dict(d) => match other {
                Dict(o) => {
                    if d.fields.iter().all(|field| {
                        field
                            .name
                            .as_ref()
                            .is_none_or(|n| o.get(n).is_some_and(|o| field.accepts(o).is_ok()))
                    }) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
                And(a) => {
                    if a.iter().all(|v| self.accepts(v).is_ok()) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
                Or(o) => {
                    if o.iter().any(|v| self.accepts(v).is_ok()) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
                _ => self.type_mismatch_error(other),
            },
            And(a) => {
                if a.iter().all(|v| v.accepts(other).is_ok()) {
                    return Ok(());
                }
                self.type_mismatch_error(other)
            }
            Or(o) => {
                if o.iter().any(|v| v.accepts(other).is_ok()) {
                    return Ok(());
                }
                self.type_mismatch_error(other)
            }
            Tuple(t) => match other {
                Tuple(o) => {
                    if o.fields.len() != t.fields.len() {
                        return Err(format!(
                            "Type {:?} with length {} cannot accept {:?} with {}",
                            t,
                            t.fields.len(),
                            o,
                            o.fields.len()
                        ));
                    }

                    if !zip(t.fields.iter(), o.fields.iter()).all(|(a, b)| a.accepts(b).is_ok()) {
                        return self.type_mismatch_error(other);
                    }
                    if zip(t.names.iter(), o.names.iter()).all(|(a, b)| a == b) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
                Array(o) => {
                    if let Some(length) = o.length {
                        if length < t.fields.len() as i32 {
                            // incoming array is shorter than current tuple
                            return Err(format!(
                                "Type mismatch {t:?} cannot accept {o:?} as incoming is shorter than this station"
                            ));
                        }
                    } else {
                        return self.type_mismatch_error(other);
                    }
                    if t.fields.iter().all(|f| f.accepts(&o.fields).is_ok()) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
                And(a) => {
                    if a.iter().all(|v| self.accepts(v).is_ok()) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
                Or(o) => {
                    if o.iter().any(|v| self.accepts(v).is_ok()) {
                        return Ok(());
                    }
                    self.type_mismatch_error(other)
                }
                _ => self.type_mismatch_error(other),
            },
        }
    }

    fn type_mismatch_error(&self, other: &OutputType) -> Result<(), String> {
        Err(format!("Type mismatch {self:?} cannot accept {other:?}"))
    }

    fn value_type(&self) -> ValType {
        match self {
            Integer => ValType::Integer,
            Float => ValType::Float,
            Text => ValType::Text,
            Boolean => ValType::Bool,
            Any => ValType::Any,
            Array(_) => ValType::Array,
            Dict(_) => ValType::Dict,
            Tuple(_) => ValType::Tuple,
            And(a) => a.first().unwrap().value_type(),
            Or(o) => o.first().unwrap().value_type(),
            OutputType::Time => ValType::Time,
            OutputType::Date => ValType::Date,
        }
    }
}

impl OutputType {
    fn parse(string: &str) -> OutputType {
        match string.to_lowercase().as_str() {
            "int" | "integer" | "i" => Integer,
            "float" | "f" => Float,
            "text" | "string" | "t" => Text,
            "bool" | "boolean" | "b" => Boolean,
            "any" => Any,
            _ => panic!("Cannot transform output"),
        }
    }
}

impl From<&Value> for OutputType {
    fn from(value: &Value) -> Self {
        match value {
            Value::Int(_) => Integer,
            Value::Float(_) => Float,
            Value::Bool(_) => Boolean,
            Value::Text(_) => Text,
            Value::Array(a) => {
                let output = if a.values.is_empty() {
                    Any
                } else {
                    OutputType::from(&a.values.first().unwrap().clone())
                };
                let layout = Layout {
                    type_: output,
                    ..Default::default()
                };

                Array(Box::new(ArrayType {
                    fields: layout,
                    length: None,
                }))
            }
            Value::Dict(d) => {
                let mut fields = Vec::new();
                d.iter().for_each(|(k, v)| {
                    fields.push(Layout::from((k.as_str(), OutputType::from(v))));
                });
                Dict(Box::new(DictType { fields }))
            }
            Value::Null => Any,
            Value::Wagon(w) => OutputType::from(&(*w.value).clone()),
            Value::Time(_) => OutputType::Time,
            Value::Date(_) => OutputType::Date,
        }
    }
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct ArrayType {
    pub fields: Layout,
    pub length: Option<i32>,
}

impl ArrayType {
    pub fn new(fields: Layout, length: Option<i32>) -> Self {
        Self { fields, length }
    }
    pub(crate) fn fits(&self, array: &value::Array) -> bool {
        array.values.iter().all(|a| self.fields.fits(a))
    }
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct TupleType {
    pub names: Vec<Option<String>>,
    pub fields: Vec<Layout>,
}

impl TupleType {
    pub fn new(name_fields: Vec<(Option<String>, Layout)>) -> Self {
        let (names, fields) = name_fields.into_iter().unzip();
        Self { names, fields }
    }

    pub(crate) fn fits(&self, array: &value::Array) -> bool {
        array.values.len() == self.names.len()
            && array
                .values
                .iter()
                .zip(self.fields.iter())
                .all(|(element, layout)| layout.fits(element))
    }
}

impl From<Vec<Layout>> for TupleType {
    fn from(value: Vec<Layout>) -> Self {
        let (names, fields) = value.into_iter().map(|l| (l.name.clone(), l)).unzip();
        TupleType { names, fields }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DictType {
    pub fields: Vec<Layout>, // "name" -> Value // we do not know if name is dynamically assigned or static
}

impl PartialEq for DictType {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl Hash for DictType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fields.hash(state);
    }
}

impl Eq for DictType {}

impl DictType {
    pub fn new(fields: Vec<Layout>) -> Self {
        DictType { fields }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.fields
            .iter()
            .any(|f| f.name.as_ref().is_some_and(|n| n == key))
    }

    pub fn get(&self, key: &str) -> Option<&Layout> {
        self.fields
            .iter()
            .find(|f| f.name.as_ref().is_some_and(|n| n == key))
    }

    pub fn names(&self) -> Vec<&str> {
        self.fields
            .iter()
            .filter_map(|l| l.name.as_deref())
            .collect()
    }

    pub(crate) fn fits(&self, dict: &Value) -> bool {
        let dict = match dict.as_dict() {
            Ok(dict) => dict,
            Err(..) => return false,
        };

        self.fields.iter().all(|l| {
            l.name
                .as_ref()
                .is_none_or(|name| dict.get(name).is_some_and(|v| l.fits(v)))
        })
    }
}


pub trait Layoutable {
    fn derive_input_layout(&self) -> Option<Layout>;

    fn derive_output_layout(&self, inputs: HashMap<String, Layout>) -> Option<Layout>;
}

#[derive(Debug, PartialEq, Clone)]
pub struct ShadowKey {
    name: String,
    alternative: Option<String>,
}

#[cfg(test)]
mod test {
    use crate::processing::layout::Layout;
    use crate::processing::layout::OutputType::{Array, Dict, Float, Integer, Text};
    use crate::processing::OutputType::Tuple;
    use crate::processing::{Plan, TupleType};
    use std::collections::HashMap;
    use std::vec;

    #[test]
    fn scalar() {
        let stencil = "f";
        let field = Layout::parse(stencil);
        assert_eq!(field.type_, Float)
    }

    #[test]
    fn scalar_nullable() {
        let stencil = "f?";
        let field = Layout::parse(stencil);
        assert_eq!(field.type_, Float);
        assert!(field.nullable);
    }

    #[test]
    fn scalar_optional_nullable() {
        let stencil = "f'?";
        let field = Layout::parse(stencil);
        assert_eq!(field.type_, Float);
        assert!(field.nullable);
        assert!(field.optional);
    }

    #[test]
    fn array() {
        let field = Layout::parse("[f]");
        match field.clone().type_ {
            Array(array) => {
                assert_eq!(array.fields.type_, Float);
                assert_eq!(array.length, None);
            }
            _ => panic!("Wrong output format"),
        }
    }

    #[test]
    fn array_nullable() {
        let stencil = "[f]?";
        let field = Layout::parse(stencil);
        match field.clone().type_ {
            Array(array) => {
                assert_eq!(array.fields.type_, Float);
            }
            _ => panic!("Wrong output format"),
        }
        assert!(field.nullable);
    }

    #[test]
    fn array_length() {
        let stencil = "[f:3]";
        let field = Layout::parse(stencil);
        match field.clone().type_ {
            Array(array) => {
                assert_eq!(array.fields.type_, Float);
            }
            _ => panic!("Wrong output format"),
        }
    }

    #[test]
    fn dict() {
        let stencils = vec!["{address: {num:i, street:t}, age: i}"];
        for stencil in stencils {
            let field = Layout::parse(stencil);
            match field.clone().type_ {
                Dict(d) => {
                    assert!(d.contains_key("address"));
                    match d.get("address").cloned().unwrap().type_ {
                        Dict(dict) => {
                            assert_eq!(dict.get("num").unwrap().type_, Integer);
                            assert_eq!(dict.get("street").unwrap().type_, Text);
                        }
                        _ => panic!("Wrong output dict"),
                    }
                    assert!(d.contains_key("age"));
                    assert_eq!(d.get("age").cloned().map(|e| e.type_).unwrap(), Integer);
                }
                _ => panic!("Wrong output format"),
            }
        }
    }

    #[test]
    fn test_two_stations_any_match() {
        let stencil = "1--2{sql|SELECT * FROM $1}"; // station 2 can accept anything
        let plan = Plan::parse(stencil).unwrap();

        assert!(plan.layouts_match().is_ok());

        let station_1 = plan.stations.get(&1).unwrap();
        assert_eq!(station_1.derive_input_layout(), Layout::default());
        assert_eq!(
            station_1.derive_output_layout(HashMap::new()),
            Layout::default()
        );

        let station_2 = plan.stations.get(&2).unwrap();
        assert_eq!(station_2.derive_input_layout(), Layout::default());
        assert_eq!(
            station_2.derive_output_layout(single_key("1", Layout::default())),
            Layout::default()
        );
    }

    #[test]
    fn test_station_literal_number() {
        let stencil = "1{sql|SELECT 1}";
        let plan = Plan::parse(stencil).unwrap();

        let station_1 = plan.stations.get(&1).unwrap();
        assert_eq!(station_1.derive_input_layout(), Layout::default());
        assert_eq!(
            station_1.derive_output_layout(HashMap::new()),
            Layout::from(Integer)
        );
    }

    #[test]
    fn test_station_literal_array() {
        let stencil = "1{sql|SELECT ['test']}";
        let plan = Plan::parse(stencil).unwrap();

        let station_1 = plan.stations.get(&1).unwrap();
        assert_eq!(station_1.derive_input_layout(), Layout::default());
        assert_eq!(
            station_1.derive_output_layout(HashMap::new()),
            Layout::from(Tuple(Box::new(TupleType::from(vec![Layout::from(Text)]))))
        );
    }

    #[test]
    fn test_two_stations_array_match() {
        let stencil = "1{sql|SELECT ['test']}--2{sql|SELECT $1.0 FROM $1}";
        let plan = Plan::parse(stencil).unwrap();

        assert!(plan.layouts_match().is_ok());
    }

    #[test]
    fn test_two_stations_array_no_match_length() {
        let stencil = "1{sql|SELECT ['test']}--2{sql|SELECT $1.1 FROM $1}";
        let plan = Plan::parse(stencil).unwrap();

        assert!(plan.layouts_match().is_err());
    }

    #[test]
    fn test_two_stations_dict_match() {
        let stencil =
            "1{sql|SELECT {'key1': 38, 'key2': 'test'}}--2{sql|SELECT [$1.key1, $1.key2]  FROM $1}";
        let plan = Plan::parse(stencil).unwrap();

        match plan.layouts_match() {
            Ok(_) => {}
            Err(e) => println!("{}", e),
        }

        assert!(plan.layouts_match().is_ok());
    }

    fn single_key<'a>(key: &str, value: Layout) -> HashMap<String, Layout> {
        HashMap::from([(key.to_string(), value)])
    }
}
