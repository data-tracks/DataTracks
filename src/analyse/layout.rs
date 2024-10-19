use std::alloc::Layout;

pub trait Layoutable {
    fn derive_layout(&self) -> Layout;
}