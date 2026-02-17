extern crate alloc;
extern crate core;

#[allow(dead_code, unused_imports)]
#[path = "value_generated.rs"]
pub mod value_generated;

pub use value_generated::data_model as flatbuf;

pub use array::Array;
pub use bool::Bool;
pub use dict::Dict;
pub use float::Float;
pub use int::Int;
pub use text::Text;
pub use time::Time;
pub use r#type::ValType;
pub use value::Value;

mod array;
mod bool;
mod date;
mod dict;
pub mod edge;
pub mod event;
pub mod float;
mod int;
pub mod node;
mod text;
mod time;
pub mod timeunit;
mod r#type;
pub mod wagon;
mod conversion;
mod math;
pub(crate) mod value;
pub mod message;
