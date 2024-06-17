use crate::algebra::filter::TrainFilter;
use crate::algebra::join::TrainJoin;
use crate::algebra::project::TrainProject;
use crate::algebra::scan::TrainScan;
use crate::processing::Transformer;
use crate::value::Value;

pub enum AlgebraType {
    Scan(TrainScan),
    Project(TrainProject),
    Filter(TrainFilter),
    Join(TrainJoin<Value>),
}

impl<'a> Algebra for AlgebraType {
    fn get_handler(&self) -> Transformer {
        match self {
            AlgebraType::Scan(s) => s.get_handler(),
            AlgebraType::Project(p) => p.get_handler(),
            AlgebraType::Filter(f) => f.get_handler(),
            AlgebraType::Join(j) => j.get_handler()
        }
    }
}

pub(crate) trait Algebra {
    fn get_handler(&self) -> Transformer;
}

pub fn funtionize(algebra: AlgebraType) -> Result<Transformer, String> {
    Ok(algebra.get_handler())
}



