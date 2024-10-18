use std::collections::HashMap;
use std::process::id;

#[derive(Default)]
pub struct Summery{
    status: Status,
    stops: HashMap<i64, Status>,
    ins: HashMap<i64, Status>,
    outs: HashMap<i64, Status>,
    complex: Vec<(Status, Vec<i64>)>,
}

impl Summery{
    pub fn new() -> Self{
        Default::default()
    }

    pub fn is_ok(&self) -> bool{
        self.status.is_ok() && self.stops.iter().all(|(_, stop)| stop.is_ok()) && self.complex.iter().all(|(status, _lines)| status.is_ok())
    }

    pub(crate) fn get_warnings(&self) -> Vec<Status> {
        let mut warnings = Vec::new();
        if !self.is_ok(){
            warnings.push(self.status.clone());
        }
        self.ins.iter().for_each(|(_, ins)|{
           if ins.is_ok() {
               warnings.push(ins.clone());
           }
        });

        self.outs.iter().for_each(|(_, outs)|{
            if outs.is_ok() {
                warnings.push(outs.clone());
            }
        });

        self.stops.values().for_each(|stop|{
            if stop.is_ok() {
                warnings.push(stop.clone());
            }
        });

        self.complex.iter().for_each(|(status, lines)| {
            if !status.is_ok(){
                warnings.push(status.clone());
            }
        });
        warnings
    }

    pub fn set_status(&mut self, status: Status){
        self.status = status;
    }

    pub fn add_stop_status(&mut self, id: i64,  status: Status){
        self.stops.insert(id, status);
    }

    pub fn add_in_status(&mut self, id: i64, status: Status){
        self.ins.insert(id, status);
    }

    pub fn add_out_status(&mut self, id: i64, status: Status){
        self.outs.insert(id, status);
    }

    pub fn add_complex_status(&mut self, status: Status, ids: Vec<i64>){
        self.complex.push((status, ids));
    }
}

#[derive(Default, Clone)]
pub enum Status {
    #[default]
    OK,
    WARNING(StatusTypes, String),
    ERROR(StatusTypes, String),
}

impl Status {
    fn is_ok(&self) -> bool {
        match self {
            Status::OK => true,
            Status::WARNING(_,_) | Status::ERROR(_,_) => false
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum StatusTypes{
    Islands
}