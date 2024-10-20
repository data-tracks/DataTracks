use std::collections::HashMap;

use crate::processing::layout::OutputType::{Any, Array, Boolean, Dict, Float, Integer, Text};
use crate::processing::plan::PlanStage;
use crate::processing::Train;
use crate::util::BufferedReader;
use crate::value;
use crate::value::{ValType, Value};

const ARRAY_OPEN: char = '[';
const DICT_OPEN: char = '{';


#[derive(Default, Clone)]
pub struct Layout {
    field: Field
}

impl Layout{

    pub(crate) fn fits(&self, train: &Train) -> bool {
        train.values.as_ref().map_or(false, |v| {
            v.iter().all(|value| self.field.fits(value))
        })
    }
}


#[derive(PartialEq, Debug, Clone)]
pub(crate) struct Field {
    explicit: bool,
    nullable: bool,
    optional: bool,
    name: Option<String>,
    position: Option<i32>,
    type_: OutputType
}


impl Default for Field {
    fn default() -> Self {
        Field {
            explicit: false,
            nullable: false,
            optional: false,
            name: None,
            position: None,
            type_: Any,
        }
    }
}

impl Field {
    pub(crate) fn parse(stencil: &str) -> Layout {
        let mut reader = BufferedReader::new(stencil.clone().to_string());

        Layout { field: parse(&mut reader) }
    }

    pub(crate) fn fits(&self, value: &Value) -> bool {
        self.type_.fits(value)
    }
}

fn parse(reader: &mut BufferedReader) -> Field {
    reader.consume_spaces();

    if let Some(char) = reader.next() {
        match char {
            DICT_OPEN => {
                parse_dict(reader)
            }
            ARRAY_OPEN => {
                parse_array(reader)
            }
            c => {
                parse_type(reader, c)
            }
        }
    } else {
        Field::default()
    }
}

fn parse_dict_fields(reader: &mut BufferedReader) -> DictType {
    let mut builder = DictBuilder::default();

    reader.consume_if_next(PlanStage::TRANSFORM_OPEN);

    while let Some(char) = reader.next(){
        match char {
            ':' => {
                reader.consume_spaces();
                let type_char = reader.next().unwrap();
                let mut layout = parse_type(reader, type_char);
                // set also name to value
                layout.name = Some(builder.key.clone());
                builder.push_value(layout);
            }
            ',' => {
                reader.consume_spaces();
            }
            PlanStage::TRANSFORM_CLOSE => {
                break
            }
            c => builder.push_key(c)
        }
    }

    reader.consume_if_next(PlanStage::TRANSFORM_CLOSE);


    DictType { fields: builder.build_fields() }
}

#[derive(Default)]
struct DictBuilder{
    fields: HashMap<String, Field>,
    key: String,
}

impl DictBuilder {
    fn push_key(&mut self, char:char){
        self.key.push(char)
    }

    fn push_value(&mut self, layout: Field) {
        self.fields.insert(self.key.clone(), layout);
        self.key.clear()
    }

    fn build_fields(&mut self) -> HashMap<String, Field> {
        let fields = self.fields.clone();
        self.fields.clear();
        fields
    }
}

fn parse_type(reader: &mut BufferedReader, c: char) -> Field {
    match c {
        'i' => parse_field(Integer, reader).0,
        'f' => parse_field(Float, reader).0,
        't' => parse_field(Text, reader).0,
        'b' => parse_field(Boolean, reader).0,
        'a' => parse_array(reader),
        'd' => parse_dict(reader),
        _ => panic!("Unknown output prefix")
    }
}

fn parse_dict(reader: &mut BufferedReader) -> Field {
    let (mut field, _values) = parse_field(Any, reader);
    reader.consume_spaces();

    field.type_ = Dict(Box::new(parse_dict_fields(reader)));
    field

}

fn parse_array(reader: &mut BufferedReader) -> Field {
    let (mut field, values) = parse_field(Any, reader);
    let length = values.get("length").map(|p|p.parse::<i32>().unwrap());
    let char_type = reader.next().unwrap();
    let fields = parse_type(reader, char_type);
    field.type_ = Array(Box::new(ArrayType { fields, length }));
    field
}

fn parse_field(type_: OutputType, reader: &mut BufferedReader) -> (Field, HashMap<String, String>) {
    let mut temp = String::default();
    let mut nullable = false;
    let mut optional = false;

    while let Some(char) = reader.peek_next() {
        match char {
            ' ' => {
                reader.next();
            },
            '?' => nullable = true,
            '\'' => optional = true,
            '(' => {
                temp.push_str(&(reader.consume_until(PlanStage::LAYOUT_CLOSE).as_str().to_owned() + &PlanStage::LAYOUT_CLOSE.to_string()));
                break;
            },
            _ => break
        };
        reader.next();
    }

    let value = parse_json(temp);
    let name = value.get("name").map(|n|n.to_string());
    let position = value.get("position").map(|p|p.parse::<i32>().unwrap());


    (Field {
        explicit: true,
        nullable,
        optional,
        name,
        position,
        type_,
    }, value)
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
    Dict(Box<DictType>)
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

    fn value_type(&self) -> ValType {
        match self {
            Integer => ValType::Integer,
            Float => ValType::Float,
            Text => ValType::Text,
            Boolean => ValType::Bool,
            Any => ValType::Any,
            Array(_) => ValType::Array,
            Dict(_) => ValType::Dict
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

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct ArrayType{
    fields: Field,
    length: Option<i32>
}

impl ArrayType {
    pub(crate) fn fits(&self, array: &value::Array) -> bool {
        array.0.iter().all(|a| self.fields.fits(a))
    }
}


#[derive(Debug, PartialEq, Clone, Default)]
pub(crate) struct DictType {
    fields: HashMap<String, Field>, // "name" -> Value
}

impl DictType {

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
    use crate::processing::layout::Field;
    use crate::processing::layout::OutputType::{Array, Dict, Float, Integer, Text};

    #[test]
    fn scalar(){
        let stencil = "f";
        let field = Field::parse(stencil);
        assert_eq!(field.field.type_, Float)
    }

    #[test]
    fn scalar_nullable(){
        let stencil = "f?";
        let field = Field::parse(stencil);
        assert_eq!(field.field.type_, Float);
        assert!(field.field.nullable);
    }

    #[test]
    fn scalar_optional(){
        let stencil = "f'?";
        let field = Field::parse(stencil);
        assert_eq!(field.field.type_, Float);
        assert!(field.field.nullable);
        assert!(field.field.optional);
    }

    #[test]
    fn array(){
        let field = Field::parse("[]");
        match field.field.clone().type_ {
            Array(array) => {
                assert_eq!(array.fields.type_, Float);
                assert_eq!(array.length, None);
            }
            _ => panic!("Wrong output format")
        }
    }

    #[test]
    fn array_length(){
        let stencil = "[]";
        let field = Field::parse(stencil);
        match field.field.clone().type_ {
            Array(array) => {

            }
            _ => panic!("Wrong output format")
        }
    }

    #[test]
    fn dict(){
        let stencils = vec!["{address: {num:i, street:t}, age: i}"];
        for stencil in stencils {
            let field = Field::parse(stencil);
            match field.field.clone().type_ {
                Dict(d) => {
                    assert!(d.fields.contains_key("address"));
                    assert_eq!(d.fields.get("address").cloned().map(|e|e.name).unwrap().unwrap(), "address");
                    match d.fields.get("address").cloned().unwrap().type_{
                        Dict(dict) => {
                            assert_eq!(dict.fields.get("num").unwrap().type_, Integer);
                            assert_eq!(dict.fields.get("street").unwrap().type_, Text);
                        }
                        _ => panic!("Wrong output dict")
                    }
                    assert!(d.fields.contains_key("age"));
                    assert_eq!(d.fields.get("age").cloned().map(|e|e.name).unwrap().unwrap(), "age");
                    assert_eq!(d.fields.get("age").cloned().map(|e|e.type_).unwrap(), Integer);
                }
                _ => panic!("Wrong output format")

            }
        }

    }

}