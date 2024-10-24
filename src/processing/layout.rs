use crate::processing::layout::OutputType::{Any, Array, Boolean, Dict, Float, Integer, Text};
use crate::processing::plan::PlanStage;
use crate::processing::Train;
use crate::util::BufferedReader;
use crate::value;
use crate::value::{ValType, Value};
use std::collections::HashMap;

const ARRAY_OPEN: char = '[';
const ARRAY_CLOSE: char = ']';
const DICT_OPEN: char = '{';
const DICT_CLOSE: char = '}';



#[derive(PartialEq, Debug, Clone)]
pub struct Layout {
    pub explicit: bool,
    pub nullable: bool,
    pub optional: bool,
    pub type_: OutputType
}


impl Default for Layout {

    fn default() -> Self {
        Layout {
            explicit: false,
            nullable: false,
            optional: false,
            type_: Any,
        }
    }
}

impl Layout {

    pub fn new(type_: OutputType) -> Layout {
        let mut field = Layout::default();
        field.type_ = type_;
        field
    }

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

    pub(crate) fn merge(&self, other: &Layout) -> Result<Layout, String> {
        let mut layout = self.clone();
        if other.explicit && self.explicit != other.explicit {
            layout = other.clone();
        }

        if other.nullable && self.nullable != other.nullable {
            return Err("Mismatch between nullable and not".to_owned())
        }


        match (&self.type_, &other.type_) {
            (Dict(this), Dict(other)) => {
                let mut dict = HashMap::new();
                this.fields.iter().for_each(|(k,v)| {dict.insert(k.clone(), v.clone());});
                other.fields.iter().for_each(|(k,v)| {dict.insert(k.clone(), v.clone());});
                layout.type_ = Dict(Box::new(DictType::new(dict)))
            }
            (this, Any) | (Any, this) => {
                layout.type_ = this.clone();
            }
            (_this, _other) => {
                todo!()
                //layout.type_ = Or(vec![this.clone(), other.clone()]);
            }
        }
        Ok(layout)
    }


    pub(crate) fn parse(stencil: &str) -> Layout {
        let mut reader = BufferedReader::new(stencil.to_string());

        parse(&mut reader)
    }

    pub(crate) fn fits(&self, value: &Value) -> bool {
        self.type_.fits(value)
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

    while let Some(char) = reader.next(){
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
pub struct DictBuilder{
    fields: HashMap<String, Layout>,
    key: String,
}

impl DictBuilder {
    pub fn push_key(&mut self, char:char){
        self.key.push(char)
    }

    pub fn push_value(&mut self, layout: Layout) {
        self.fields.insert(self.key.clone(), layout);
        self.key.clear()
    }

    pub fn build_fields(&mut self) -> HashMap<String, Layout> {
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
        prefix => panic!("Unknown output prefix: {}", prefix )
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
    }else {
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
            ' ' => {
            },
            '?' => nullable = true,
            '\'' => optional = true,
            ':' => {
                let mut num = String::new();
                while let Some(char) = reader.peek_next() {
                    if let Some(_) = char.to_digit(10 ) {
                        num.push(char);
                        reader.next();
                    }else {
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
        explicit: true,
        nullable,
        optional,
        type_,
    }, length)
}

fn parse_json(mut string: String) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if string.trim().is_empty() {
        return map
    }
    string.pop();
    string.remove(0);
    string = string.trim().into();
    if string.is_empty() {
        return map
    }

    string.split(',').for_each(|pair| {
        let key_value = pair.split(':').collect::<Vec<&str>>();
        map.insert(key_value.first().unwrap().trim().to_string(), key_value.get(1).unwrap().trim().to_string());
    });
    map
}

#[derive(PartialEq, Debug, Clone)]
pub enum OutputType {
  // single value
    Integer,
    Float,
    Text,
    Boolean,
    Any,
    Array(Box<ArrayType>),
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
            Integer | Float | Text | Boolean => {
                match other {
                    Integer | Float | Text | Boolean => true,
                    Any | Array(_) | Dict(_) => false,
                    OutputType::And(a) => a.iter().all(|v| self.accepts(v)),
                    OutputType::Or(o) => o.iter().any(|v| self.accepts(v))
                }
            }
            Any => {
                true
            }
            Array(a) => {
                match other {
                    Array(other) => a.fields.accepts(&other.fields),
                    OutputType::And(a) => a.iter().all(|v| self.accepts(v)),
                    OutputType::Or(o) => o.iter().any(|v| self.accepts(v)),
                    _ => false
                }
            }
            Dict(d) => {
                match other {
                    Dict(other) => {
                        for (key, value) in &d.fields {
                            if let Some(other) = other.fields.get(key) {
                                if !value.accepts(other) {
                                    return false;
                                }
                            } else {
                                return false
                            }
                        }
                        true
                    }
                    OutputType::And(a) => a.iter().all(|v| self.accepts(v)),
                    OutputType::Or(o) => o.iter().any(|v| self.accepts(v)),
                    _ => false
                }
            }
            OutputType::And(a) => {
                a.iter().all(|v| v.accepts(other))
            }
            OutputType::Or(o) => {
                o.iter().any(|v| v.accepts(other))
            }
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
            OutputType::And(a) => a.first().unwrap().value_type(),
            OutputType::Or(o) => o.first().unwrap().value_type(),
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
                }else {
                    OutputType::from(&a.0.first().unwrap().clone())
                };
                let mut layout = Layout::default();
                layout.type_ = output;

                Array(Box::new(ArrayType{ fields: layout, length: None}))
            },
            Value::Dict(d) => {
                let mut fields = HashMap::new();
                d.iter().for_each(|(k,v)|{
                    fields.insert(k.clone(), Layout::new(OutputType::from(v)));
                });
                Dict(Box::new(DictType{fields}))
            }
            Value::Null => {
                Any
            }
            Value::Wagon(w) => OutputType::from(&(*w.value).clone()),
        }
    }
}


#[derive(PartialEq, Debug, Clone)]
pub struct ArrayType{
    pub fields: Layout,
    pub length: Option<i32>
}

impl ArrayType {

    pub fn new(fields: Layout, length: Option<i32>) -> Self {
        Self { fields, length }
    }
    pub(crate) fn fits(&self, array: &value::Array) -> bool {
        array.0.iter().all(|a| self.fields.fits(a))
    }
}


#[derive(Debug, PartialEq, Clone, Default)]
pub struct DictType {
    pub fields: HashMap<String, Layout>, // "name" -> Value
}

impl DictType {

    pub fn new(fields: HashMap<String, Layout>) -> Self {
        DictType{fields}
    }

    pub(crate) fn fits(&self, dict: &Value) -> bool {
        for (name, field) in self.fields.iter().by_ref() {
            if let Some(value) = dict.as_dict().unwrap().get(name) {
                if !field.fits(value) {
                    return false
                }
            } else {
                return false
            }
        }
        true
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

    #[test]
    fn scalar(){
        let stencil = "f";
        let field = Layout::parse(stencil);
        assert_eq!(field.type_, Float)
    }

    #[test]
    fn scalar_nullable(){
        let stencil = "f?";
        let field = Layout::parse(stencil);
        assert_eq!(field.type_, Float);
        assert!(field.nullable);
    }

    #[test]
    fn scalar_optional_nullable(){
        let stencil = "f'?";
        let field = Layout::parse(stencil);
        assert_eq!(field.type_, Float);
        assert!(field.nullable);
        assert!(field.optional);
    }

    #[test]
    fn array(){
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
    fn array_nullable(){
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
    fn array_length(){
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
    fn dict(){
        let stencils = vec!["{address: {num:i, street:t}, age: i}"];
        for stencil in stencils {
            let field = Layout::parse(stencil);
            match field.clone().type_ {
                Dict(d) => {
                    assert!(d.fields.contains_key("address"));
                    match d.fields.get("address").cloned().unwrap().type_{
                        Dict(dict) => {
                            assert_eq!(dict.fields.get("num").unwrap().type_, Integer);
                            assert_eq!(dict.fields.get("street").unwrap().type_, Text);
                        }
                        _ => panic!("Wrong output dict")
                    }
                    assert!(d.fields.contains_key("age"));
                    assert_eq!(d.fields.get("age").cloned().map(|e|e.type_).unwrap(), Integer);
                }
                _ => panic!("Wrong output format")

            }
        }

    }

}