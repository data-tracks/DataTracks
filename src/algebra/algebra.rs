use crate::algebra::filter::TrainFilter;
use crate::algebra::join::TrainJoin;
use crate::algebra::project::TrainProject;
use crate::algebra::scan::TrainScan;
use crate::processing::{Referencer, Train};
use crate::value::Value;

pub enum AlgebraType {
    Scan(TrainScan),
    Project(TrainProject),
    Filter(TrainFilter),
    Join(TrainJoin<Value>),
}

impl Algebra for AlgebraType {
    fn get_handler(&mut self) -> Box<dyn RefHandler> {
        match self {
            AlgebraType::Scan(s) => s.get_handler(),
            AlgebraType::Project(p) => p.get_handler(),
            AlgebraType::Filter(f) => f.get_handler(),
            AlgebraType::Join(j) => j.get_handler()
        }
    }
}

pub(crate) trait Algebra {
    fn get_handler(&mut self) -> Box<dyn RefHandler>;
}

pub fn functionize(mut algebra: AlgebraType) -> Result<Box<dyn RefHandler>, String> {
    Ok(algebra.get_handler())
}

pub trait RefHandler {
    fn process(&self, train: &mut Train) -> Train;
}

pub trait Handler {
    fn process(&self, train: Train) -> Train;
}




