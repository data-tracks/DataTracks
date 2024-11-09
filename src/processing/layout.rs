use crate::processing::layout::OutputType::{Any, Array, Boolean, Dict, Float, Integer, Text};
use crate::processing::plan::PlanStage;
use crate::processing::OutputType::Tuple;
use crate::processing::Train;
use crate::util::BufferedReader;
use crate::value;
use crate::value::{ValType, Value};
use std::cmp::min;
use std::collections::HashMap;
use std::hash::Hash;
use std::iter::zip;
use tracing::warn;
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
}


impl Default for Layout {
    fn default() -> Self {
        Layout {
            name: None,
            explicit: false,
            nullable: false,
            optional: false,
            type_: Any,
        }
    }
}

impl Layout {
    pub fn fits_train(&self, train: &Train) -> bool {
        train.values.as_ref().map_or(false, |v| {
            v.iter().all(|value| self.fits(value))
        })
    }

    pub(crate) fn accepts(&self, other: &Layout) -> bool {
        if !self.nullable && other.nullable {
            return false;
        }
        if !self.optional && other.optional {
            return false;
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

                this.fields.iter().for_each(|f| {
                    dict.push(f.clone())
                });
                other.fields.iter().filter(|f| f.name.as_ref().map_or(true, |name| !this.contains_key(name))).for_each(|f| { dict.push(f.clone()) });
                layout.type_ = Dict(Box::new(DictType::new(dict)))
            }
            (Array(this), Array(other)) => {
                let mut array = ArrayType::new(this.fields.merge(&other.fields), None);

                array.length = match (this.length, other.length) {
                    (Some(this), Some(other)) => {
                        Some(min(this, other))
                    }
                    (_, _) => {
                        None
                    }
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
                //layout.type_ = Or(vec![this.clone(), other.clone()]);
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
        Layout { type_: Array(Box::new(ArrayType::new(Layout::default(), index))), ..Default::default() }
    }

    pub fn tuple(names: Vec<Option<String>>) -> Layout {
        Layout { type_: Tuple(Box::new(TupleType::from(names.into_iter().map(|n| n.map_or(Layout::default(), |n| Layout::from(n.as_str()))).collect::<Vec<_>>()))), ..Default::default() }
    }

    pub(crate) fn fits(&self, value: &Value) -> bool {
        self.type_.fits(value)
    }
}

impl From<OutputType> for Layout {
    fn from(type_: OutputType) -> Self {
        Layout { type_, ..Default::default() }
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
        let mut layout = Layout::default();
        layout.name = Some(name.to_string());
        layout
    }
}

impl From<(&str, OutputType)> for Layout {
    fn from((name, type_): (&str, OutputType)) -> Self {
        Layout { name: Some(name.to_string()), type_, ..Default::default() }
    }
}


fn parse(reader: &mut BufferedReader) -> Layout {
    reader.consume_spaces();

    if let Some(char) = reader.next() {
        match char {
            DICT_OPEN => {
                parse_dict(reader).0
            }
            ARRAY_OPEN => {
                parse_array(reader).0
            }
            c => {
                parse_type(reader, c).0
            }
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
            DICT_CLOSE => {
                break
            }
            c => builder.push_key(c)
        }
    }

    reader.consume_if_next(DICT_CLOSE);


    DictType { fields: builder.build_fields() }
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
        prefix => panic!("Unknown output prefix: {}", prefix)
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
            ARRAY_CLOSE => {
                (Layout::default(), None)
            }
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

    field.type_ = Array(Box::new(ArrayType { fields: fields.0, length: fields.1 }));

    (field, length)
}

fn parse_field(type_: OutputType, reader: &mut BufferedReader) -> (Layout, Option<i32>) { // field : length
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

    (Layout {
        name: None,
        explicit: true,
        nullable,
        optional,
        type_,
    }, length)
}

fn parse_json(mut string: String) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if string.trim().is_empty() {
        return map;
    }
    string.pop();
    string.remove(0);
    string = string.trim().into();
    if string.is_empty() {
        return map;
    }

    string.split(',').for_each(|pair| {
        let key_value = pair.split(':').collect::<Vec<&str>>();
        map.insert(key_value.first().unwrap().trim().to_string(), key_value.get(1).unwrap().trim().to_string());
    });
    map
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub enum OutputType {
    // single value
    Integer,
    Float,
    Text,
    Boolean,
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
            Array(a) => {
                match value {
                    Value::Array(array) => {
                        a.fits(array)
                    }
                    _ => false
                }
            }
            Dict(d) => {
                match value {
                    Value::Dict(dict) => {
                        d.fits(&Value::Dict(dict.clone()))
                    }
                    _ => false
                }
            }
            t => {
                value.type_() == t.value_type()
            }
        }
    }

    pub(crate) fn accepts(&self, other: &OutputType) -> bool {
        match self {
            Integer | Float | Text | Boolean => match other {
                Integer | Float | Text | Boolean => true,
                Any | Array(_) | Dict(_) | Tuple(_) => false,
                And(a) => a.iter().all(|v| self.accepts(v)),
                Or(o) => o.iter().any(|v| self.accepts(v)),
            },
            Any => {
                true
            }
            Array(a) => {
                match other {
                    Array(other) => {
                        if !a.fields.accepts(&other.fields) {
                            return false;
                        }
                        if a.length.is_some() && other.length.is_none() {
                            return false;
                        }
                        if a.length.is_none() && other.length.is_none() {
                            return true;
                        }
                        a.length.unwrap() <= other.length.unwrap()
                    }
                    Tuple(t) => {
                        if let Some(length) = a.length {
                            if length > t.fields.len() as i32 {
                                return false;
                            }
                        }

                        t.fields.iter().all(|t| a.fields.accepts(t))
                    },
                    And(a) => a.iter().all(|v| self.accepts(v)),
                    Or(o) => o.iter().any(|v| self.accepts(v)),
                    _ => false
                }
            }
            Dict(d) => {
                match other {
                    Dict(other) => {
                        d.fields.iter().all(|field| {
                            field.name.as_ref().map_or(true, |n| {
                                other.get(n).map_or(false, |other| {
                                    field.accepts(other)
                                })
                            })
                        })
                    }
                    And(a) => a.iter().all(|v| self.accepts(v)),
                    Or(o) => o.iter().any(|v| self.accepts(v)),
                    _ => false
                }
            }
            And(a) => {
                a.iter().all(|v| v.accepts(other))
            }
            Or(o) => {
                o.iter().any(|v| v.accepts(other))
            }
            Tuple(t) => match other {
                Tuple(other) => {
                    if other.fields.len() != t.fields.len() {
                        return false;
                    }

                    if !zip(t.fields.iter(), other.fields.iter()).all(|(a, b)| a == b) {
                        return false;
                    }
                    zip(t.names.iter(), other.names.iter()).all(|(a, b)| a == b)
                }
                Array(other) => {
                    if let Some(length) = other.length {
                        if length < t.fields.len() as i32 {
                            // incoming array is shorter than current tuple
                            return false;
                        }
                    } else {
                        return false;
                    }
                    t.fields.iter().all(|f| f.accepts(&other.fields))
                },
                And(a) => a.iter().all(|v| self.accepts(v)),
                Or(o) => o.iter().any(|v| self.accepts(v)),
                _ => false
            },
        }
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
            _ => panic!("Cannot transform output")
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
                let output = if a.0.is_empty() {
                    Any
                } else {
                    OutputType::from(&a.0.first().unwrap().clone())
                };
                let layout = Layout { type_: output, ..Default::default() };

                Array(Box::new(ArrayType { fields: layout, length: None }))
            }
            Value::Dict(d) => {
                let mut fields = Vec::new();
                d.iter().for_each(|(k, v)| {
                    fields.push(Layout::from((k.as_str(), OutputType::from(v))));
                });
                Dict(Box::new(DictType { fields }))
            }
            Value::Null => {
                Any
            }
            Value::Wagon(w) => OutputType::from(&(*w.value).clone()),
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
        array.0.iter().all(|a| self.fields.fits(a))
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
        array.0.len() == self.names.len() && array.0.iter().zip(self.fields.iter()).all(|(element, layout)| layout.fits(element))
    }
}

impl From<Vec<Layout>> for TupleType {
    fn from(value: Vec<Layout>) -> Self {
        let (names, fields) = value.into_iter().map(|l| (l.name.clone(), l)).unzip();
        TupleType { names, fields }
    }
}


#[derive(Debug, Clone, Default, Hash)]
pub struct DictType {
    pub fields: Vec<Layout>, // "name" -> Value // we do not know if name is dynamically assigned or static
}

impl PartialEq for DictType {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl Eq for DictType {}

impl DictType {
    pub fn new(fields: Vec<Layout>) -> Self {
        DictType { fields }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.fields.iter().any(|f| f.name.as_ref().map_or(false, |n| n == key))
    }

    pub fn get(&self, key: &str) -> Option<&Layout> {
        self.fields.iter().find(|f| f.name.as_ref().map_or(false, |n| n == key))
    }

    pub fn names(&self) -> Vec<&str> {
        self.fields.iter().filter_map(|l| l.name.as_ref().map(|n| n.as_str())).collect()
    }


    pub(crate) fn fits(&self, dict: &Value) -> bool {
        let dict = match dict.as_dict() {
            Ok(dict) => dict,
            Err(..) => return false
        };

        self.fields.iter().all(|l| {
            l.name
                .as_ref()
                .map_or(true,
                        |name| dict.get(name).map_or(false, |v| l.fits(v)))
        })
    }
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
            _ => panic!("Wrong output format")
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
            _ => panic!("Wrong output format")
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
            _ => panic!("Wrong output format")
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
                        _ => panic!("Wrong output dict")
                    }
                    assert!(d.contains_key("age"));
                    assert_eq!(d.get("age").cloned().map(|e| e.type_).unwrap(), Integer);
                }
                _ => panic!("Wrong output format")
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
        assert_eq!(station_1.derive_output_layout(HashMap::new()), Layout::default());

        let station_2 = plan.stations.get(&2).unwrap();
        assert_eq!(station_2.derive_input_layout(), Layout::default());
        assert_eq!(station_2.derive_output_layout(single_key("1", &Layout::default())), Layout::default());
    }


    #[test]
    fn test_station_literal_number() {
        let stencil = "1{sql|SELECT 1}";
        let plan = Plan::parse(stencil).unwrap();

        let station_1 = plan.stations.get(&1).unwrap();
        assert_eq!(station_1.derive_input_layout(), Layout::default());
        assert_eq!(station_1.derive_output_layout(HashMap::new()), Layout::from(Integer));
    }

    #[test]
    fn test_station_literal_array() {
        let stencil = "1{sql|SELECT ['test']}";
        let plan = Plan::parse(stencil).unwrap();

        let station_1 = plan.stations.get(&1).unwrap();
        assert_eq!(station_1.derive_input_layout(), Layout::default());
        assert_eq!(station_1.derive_output_layout(HashMap::new()), Layout::from(Tuple(Box::new(TupleType::from(vec![Layout::from(Text)])))));
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
        let stencil = "1{sql|SELECT {'key1': 38, 'key2': 'test'}}--2{sql|SELECT [$1.key1, $1.key2]  FROM $1}";
        let plan = Plan::parse(stencil).unwrap();

        match plan.layouts_match() {
            Ok(_) => {}
            Err(e) => println!("{}", e)
        }

        assert!(plan.layouts_match().is_ok());
    }


    fn single_key<'a>(key: &str, value: &'a Layout) -> HashMap<String, &'a Layout> {
        HashMap::from([(key.to_string(), value)])
    }
}