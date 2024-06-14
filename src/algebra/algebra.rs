use crate::processing::Train;

pub enum AlgebraType {}

pub trait Algebra {
    fn get_handler(&self) -> Box<dyn Fn() -> Train>;
}




