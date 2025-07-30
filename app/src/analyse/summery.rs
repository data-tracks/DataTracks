use std::collections::HashMap;
use std::fmt::Display;

#[derive(Default)]
pub struct Summery {
    status: Status,
    stops: HashMap<usize, Status>,
    ins: HashMap<usize, Status>,
    outs: HashMap<usize, Status>,
    complex: Vec<(Status, Vec<usize>)>,
}

impl Summery {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn is_ok(&self) -> bool {
        self.status.is_ok()
            && self.stops.iter().all(|(_, stop)| stop.is_ok())
            && self.complex.iter().all(|(status, _lines)| status.is_ok())
    }

    pub(crate) fn get_warnings(&self) -> Vec<Status> {
        let mut warnings = Vec::new();
        if !self.status.is_ok() {
            warnings.push(self.status.clone());
        }
        self.ins.iter().for_each(|(_, ins)| {
            if !ins.is_ok() {
                warnings.push(ins.clone());
            }
        });

        self.outs.iter().for_each(|(_, outs)| {
            if !outs.is_ok() {
                warnings.push(outs.clone());
            }
        });

        self.stops.values().for_each(|stop| {
            if !stop.is_ok() {
                warnings.push(stop.clone());
            }
        });

        self.complex.iter().for_each(|(status, _lines)| {
            if !status.is_ok() {
                warnings.push(status.clone());
            }
        });
        warnings
    }

    pub fn add_stop_status(&mut self, id: usize, status: Status) {
        self.stops.insert(id, status);
    }

    pub fn add_in_status(&mut self, id: usize, status: Status) {
        self.ins.insert(id, status);
    }

    pub fn add_out_status(&mut self, id: usize, status: Status) {
        self.outs.insert(id, status);
    }

    pub fn add_complex_status(&mut self, status: Status, ids: Vec<usize>) {
        self.complex.push((status, ids));
    }
}

impl Display for Summery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{}", self.status).as_str())
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub enum Status {
    #[default]
    Ok,
    Warning(StatusTypes, String),
    Error(StatusTypes, String),
}

impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Status::Ok => "Ok",
            Status::Warning(t, print) => &format!("{t}\n{print}"),
            Status::Error(t, print) => &format!("{t}\n{print}"),
        };
        f.write_str(format!("\n{name}\n").as_str())
    }
}

impl Status {
    fn is_ok(&self) -> bool {
        match self {
            Status::Ok => true,
            Status::Warning(_, _) | Status::Error(_, _) => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum StatusTypes {
    Islands,
}

impl Display for StatusTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            StatusTypes::Islands => "Islands",
        };
        f.write_str(name)
    }
}
