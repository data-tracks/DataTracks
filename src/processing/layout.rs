use std::collections::HashMap;
use serde::de::Unexpected::Str;
use crate::processing::layout::OutputType::{Any, Array, Boolean, Float, Integer, Text, Tuple};
use crate::util::BufferedReader;

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct Field {
    explicit: bool,
    name: Option<String>,
    position: Option<i32>,
    type_: OutputType
}

impl Default for Field {
    fn default() -> Self {
        Field{
            explicit: false,
            name: None,
            position: None,
            type_: Any,
        }
    }
}

#[derive(PartialEq)]
enum FieldStage{
    ScalarStage,
    ArrayStage,
    TupleStage,
    Initial
}

impl Field{
    pub(crate) fn parse(stencil: String) -> Field{
        let mut reader = BufferedReader::new(stencil);


        while let Some(char) = reader.next() {
            match char {
                '{' => {
                    return parse_tuple(&mut reader, None)
                },
                '(' => {
                    return parse_scalar(&mut reader, None)
                },
                '[' => {
                    return parse_array(&mut reader, None)
                },
                ' ' => { },
                c => panic!("Unknown char in output: {}", c),
            }
        }
        panic!("Could not parse")
    }
}

fn parse_tuple(reader: &mut BufferedReader, name: Option<String>) -> Field {
    let mut temp = String::default();
    let mut fields = HashMap::new();

    while let Some(char) = reader.next() {
        match char {
            '(' => {
                let name = temp.clone().trim().to_owned();
                fields.insert(name.clone(), parse_scalar(reader, Some(name)));
            },
            '{' => {
                let name = temp.clone().trim().to_owned();
                fields.insert(name.clone(), parse_tuple(reader, Some(name)));
            },
            '[' => {
                let name = temp.clone().trim().to_owned();
                fields.insert(name.clone(), parse_array(reader, Some(name)));
            },
            ',' => {
                temp = String::default();
            },
            ' ' | ':' => {},
            _ => temp.push(char),
        }
    }
    Field{
        explicit: false,
        name,
        position: None,
        type_: Tuple(Box::new(TupleType{ fields })),
    }
}

fn parse_array(reader: &mut BufferedReader, name: Option<String>) -> Field {
    let mut temp = String::default();
    let mut key = String::default();
    let mut is_fields = false;
    let mut field = Field::default();
    field.name = name;
    let mut array_type = ArrayType{ fields: Any, length: None };

    while let Some(char) = reader.next() {
        match char {
            '(' => {
                if !is_fields {
                    panic!("Could not parse array output")
                }
                array_type.fields = parse_scalar(reader, None).type_;
            },
            '{' => {
                if !is_fields {
                    panic!("Could not parse array output")
                }
                array_type.fields = parse_tuple(reader, None).type_;
            },
            '[' => {
                if !is_fields {
                    panic!("Could not parse array output")
                }
                array_type.fields = parse_array(reader, None).type_;
            },
            ',' => {
                if !is_fields {
                    update_properties(&mut key, temp.clone(), &mut field, &mut array_type);
                }
                temp = String::default();
                key = String::default();
                is_fields = false;
            }
            ':' => {
                key = temp.clone();
                temp = String::default();

                is_fields = key.eq_ignore_ascii_case("fields");
            },
            ']' => {
                if !is_fields {
                    update_properties(&mut key, temp.clone(), &mut field, &mut array_type);
                }

                field.type_ = Array(Box::new(array_type));
                
                return field
            },
            _ => temp.push(char),
        }
    }
    panic!("Could not parse array outlet")
}

fn update_properties(mut key: &mut String, value: String, field: &mut Field, array_type: &mut ArrayType) {
    match key.to_lowercase().trim() {
        "name" => { field.name = Some(value.trim().to_string()) },
        "position" => { field.position = Some(value.trim().parse().unwrap()) },
        "length" => array_type.length = Some(value.trim().parse().unwrap()) ,
        _ => panic!("Unknown key value pair")
    }
}

fn parse_scalar(reader: &mut BufferedReader, name: Option<String>) -> Field {
    let mut field = Field::default();
    field.name = name;
    let value = reader.consume_until(')');

    for pair in value.split(",") {
        let mut elements = vec![];
        for split in pair.split(":") {
            elements.push(split.trim());
        }
        if elements.len() != 2 {
            panic!("Not correct layout specification");
        }
        match elements.get(0).unwrap().to_lowercase().as_str() {
            "type" => {
                field.type_ = OutputType::parse(elements.get(1).unwrap());
            },
            "name" => {
                field.name = Some(elements.get(1).unwrap().parse().unwrap());
            },
            "position" => {
                field.position = Some(elements.get(1).unwrap().parse().unwrap())
            }
            _ => {}
        }

    }
    return field;

}

#[derive(PartialEq, Debug, Clone)]
pub(crate) enum OutputType {
  // single value
    Integer,
    Float,
    Text,
    Boolean,
    Any,
    Array(Box<ArrayType>),
    Tuple(Box<TupleType>)
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
    fields: OutputType,
    length: Option<i32>
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct TupleType{
    fields: HashMap<String, Field> // "name" or "0"
}



#[cfg(test)]
mod test {
    use crate::processing::layout::{Field, OutputType};
    use crate::processing::layout::OutputType::{Array, Float, Integer, Text};

    #[test]
    fn scalar(){
        let stencil = "(type: float)";
        let field = Field::parse(stencil.to_string());
        assert_eq!(field.type_, Float)
    }

    #[test]
    fn scalar_name(){
        let stencil = "(type: integer, name: test )";
        let field = Field::parse(stencil.to_string());
        assert_eq!(field.type_, Integer);
        assert_eq!(field.name.unwrap(), "test");
    }

    #[test]
    fn scalar_position(){
        let stencil = "(type: text, position: 4 )";
        let field = Field::parse(stencil.to_string());
        assert_eq!(field.type_, Text);
        assert_eq!(field.position.unwrap(), 4);
    }

    #[test]
    fn array(){
        let stencil = "[fields: (type:float)]";
        let field = Field::parse(stencil.to_string());
        match field.type_ {
            Array(array) => {
                assert_eq!(array.fields, Float);
                assert_eq!(array.length, None);
            }
            _ => panic!("Wrong output format")
        }
    }

    #[test]
    fn array_length(){
        let stencil = "[fields: (type:float), length: 3]";
        let field = Field::parse(stencil.to_string());
        match field.type_ {
            Array(array) => {
                assert_eq!(array.fields, Float);
                assert_eq!(array.length.unwrap(), 3);
            }
            _ => panic!("Wrong output format")
        }
    }

    #[test]
    fn tuple(){
        let stencil = "{name: (type:text), age: (type: int)}";
        let field = Field::parse(stencil.to_string());
        match field.type_ {
            OutputType::Tuple(t) => {
                assert!(t.fields.contains_key("name"));
                assert_eq!(t.fields.get("name").cloned().map(|e|e.name).unwrap().unwrap(), "name");
                assert_eq!(t.fields.get("name").cloned().map(|e|e.type_).unwrap(), Text);
                assert!(t.fields.contains_key("age"));
                assert_eq!(t.fields.get("age").cloned().map(|e|e.name).unwrap().unwrap(), "age");
                assert_eq!(t.fields.get("age").cloned().map(|e|e.type_).unwrap(), Integer);
            }
            _ => panic!("Wrong output format")

        }
    }

}