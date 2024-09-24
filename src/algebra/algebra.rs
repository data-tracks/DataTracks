use crate::algebra::filter::TrainFilter;
use crate::algebra::join::TrainJoin;
use crate::algebra::project::TrainProject;
use crate::algebra::scan::TrainScan;
use crate::processing::Train;
use crate::value::Value;

pub enum AlgebraType {
    Scan(TrainScan),
    Project(TrainProject),
    Filter(TrainFilter),
    Join(TrainJoin),
}

impl Algebra for AlgebraType {
    fn get_enumerator(&mut self) -> Box<dyn ValueEnumerator<Item=Value> + Send> {
        match self {
            AlgebraType::Scan(s) => s.get_enumerator(),
            AlgebraType::Project(p) => p.get_enumerator(),
            AlgebraType::Filter(f) => f.get_enumerator(),
            AlgebraType::Join(j) => j.get_enumerator()
        }
    }
}

pub trait Algebra {
    fn get_enumerator(&mut self) -> Box<dyn ValueEnumerator<Item=Value> + Send>;
}

pub fn functionize(mut algebra: AlgebraType) -> Result<Box<dyn ValueEnumerator<Item=Value> + Send + 'static>, String> {
    Ok(algebra.get_enumerator())
}

pub trait RefHandler: Send {
    fn process(&self, stop: i64, wagons: Vec<Train>) -> Vec<Train>;

    fn clone(&self) -> Box<dyn RefHandler + Send + 'static>;
}

pub trait ValueHandler: Send {
    fn process(&self, value: Value) -> Value;

    fn clone(&self) -> Box<dyn ValueHandler + Send + 'static>;
}

pub trait ValueRefHandler: Send {
    fn process(&self, value: &Value) -> Value;

    fn clone(&self) -> Box<dyn ValueRefHandler + Send + 'static>;
}


pub trait ValueEnumerator: Iterator<Item = Value> + Send + 'static {
    fn load(&mut self, trains: Vec<Train>);

    fn drain(&mut self) -> Vec<Value>{
        self.into_iter().collect()
    }

    fn drain_to_train(&mut self, stop: i64) -> Train {
        Train::new(stop, self.drain())
    }

    fn clone(&self) -> Box<dyn ValueEnumerator<Item=Value> + Send + 'static>;
}

