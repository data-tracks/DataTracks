extern crate alloc;
extern crate core;

pub mod value_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/value_capnp.rs"));
}

pub use value_capnp::value as valuecp; // Now you can use it!

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
pub mod train;
mod r#type;
pub mod wagon;
mod conversion;
mod math;
pub(crate) mod value;
