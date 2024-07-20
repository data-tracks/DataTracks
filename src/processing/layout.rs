use std::collections::HashMap;
use crate::processing::layout::OutputType::{Any, Array, Boolean, Float, Integer, Text, Tuple};
use crate::util::BufferedReader;

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
        Field{
            explicit: false,
            nullable: false,
            optional: false,
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

        if let Some(value) = try_parse(&mut reader) {
            value
        }else {
            panic!("Could not parse field")
        }
    }
}

fn try_parse(reader: &mut BufferedReader) -> Option<Field> {
    reader.consume_spaces();
    if let Some(char) = reader.next() {
        return Some(parse_type(reader, char))
    }
    None
}

fn parse_type(reader: &mut BufferedReader, c: char) -> Field {
    match c {
        'i' => parse_scalar(Integer, reader).0,
        'f' => parse_scalar(Float, reader).0,
        't' => parse_scalar(Text, reader).0,
        'b' => parse_scalar(Boolean, reader).0,
        'a' => parse_array(reader),
        'd' => parse_dict(reader),
        _ => panic!("Unknown output prefix")
    }
}

fn parse_dict(reader: &mut BufferedReader) -> Field {
    let (mut field, _values) = parse_scalar(Any, reader);
    let mut temp = String::default();

    reader.consume_spaces();
    if let Some(char ) = reader.peek_next() {
        if char == '{' {
            temp.push_str(&(reader.consume_until('}').as_str().to_owned() + "}"));
        }
    }

    let json = parse_json(temp);
    let mut fields = HashMap::new();
    for (key, value) in json {
        let mut buffered_reader = BufferedReader::new(value);
        let mut field = try_parse(&mut buffered_reader).unwrap();
        // set field name
        field.name = Some(key.clone());
        fields.insert(key, field);
    }

    field.type_ = Tuple(Box::new(DictType{ fields }));
    field

}

fn parse_array(reader: &mut BufferedReader) -> Field {
    let (mut field, values) = parse_scalar(Any, reader);
    let length = values.get("length").map(|p|p.parse::<i32>().unwrap());
    let output = match try_parse(reader) {
        None => panic!("Could not parse component"),
        Some(component) => {
            Array(Box::new(ArrayType{ fields: component.type_, length }))
        }
    };
    field.type_ = output;
    field
}

fn parse_scalar(type_: OutputType, mut reader: &mut BufferedReader) -> (Field, HashMap<String, String>) {
    let mut temp = String::default();
    let mut nullable = false;
    let mut optional = false;

    while let Some(char) = reader.peek_next() {
        match char {
            ' ' => {},
            '?' => nullable = true,
            '\'' => optional = true,
            '(' => {
                temp.push_str(&(reader.consume_until(')').as_str().to_owned() + ")"));
                break;
            },
            _ => break
        };
        reader.next();
    }

    let value = parse_json(temp);
    let name = value.get("name").map(|n|n.to_string());
    let position = value.get("position").map(|p|p.parse::<i32>().unwrap());


    (Field{
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
pub(crate) enum OutputType {
  // single value
    Integer,
    Float,
    Text,
    Boolean,
    Any,
    Array(Box<ArrayType>),
    Tuple(Box<DictType>)
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
pub(crate) struct DictType {
    fields: HashMap<String, Field> // "0"
}



#[cfg(test)]
mod test {
    use crate::processing::layout::{Field, OutputType};
    use crate::processing::layout::OutputType::{Array, Float, Integer, Text};

    #[test]
    fn scalar(){
        let stencil = "f";
        let field = Field::parse(stencil.to_string());
        assert_eq!(field.type_, Float)
    }

    #[test]
    fn scalar_nullable(){
        let stencil = "f?";
        let field = Field::parse(stencil.to_string());
        assert_eq!(field.type_, Float);
        assert!(field.nullable);
    }

    #[test]
    fn scalar_optional(){
        let stencil = "f'?";
        let field = Field::parse(stencil.to_string());
        assert_eq!(field.type_, Float);
        assert!(field.nullable);
        assert!(field.optional);
    }

    #[test]
    fn scalar_name(){
        let stencil = "i(name: test)";
        let field = Field::parse(stencil.to_string());
        assert_eq!(field.type_, Integer);
        assert_eq!(field.name.unwrap(), "test");
    }

    #[test]
    fn array(){
        let stencils = vec!["a()f", "af"];
        for stencil in stencils {
            let field = Field::parse(stencil.to_string());
            match field.type_ {
                Array(array) => {
                    assert_eq!(array.fields, Float);
                    assert_eq!(array.length, None);
                }
                _ => panic!("Wrong output format")
            }
        }
    }

    #[test]
    fn array_length(){
        let stencil = "a(length: 3)f";
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
    fn dict(){
        let stencils = vec!["d'?{name: t, age: i}", "d(optional: true, nullable: true){name: t, age: i}"];
        for stencil in stencils {
            let field = Field::parse(stencil.to_string());
            match field.type_ {
                OutputType::Tuple(d) => {
                    assert!(d.fields.contains_key("name"));
                    assert_eq!(d.fields.get("name").cloned().map(|e|e.name).unwrap().unwrap(), "name");
                    assert_eq!(d.fields.get("name").cloned().map(|e|e.type_).unwrap(), Text);
                    assert!(d.fields.contains_key("age"));
                    assert_eq!(d.fields.get("age").cloned().map(|e|e.name).unwrap().unwrap(), "age");
                    assert_eq!(d.fields.get("age").cloned().map(|e|e.type_).unwrap(), Integer);
                }
                _ => panic!("Wrong output format")

            }
        }

    }

}