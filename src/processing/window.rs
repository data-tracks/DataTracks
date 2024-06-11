use crate::processing::train::Train;
use crate::processing::window::Window::Back;

pub enum Window {
    Back(BackWindow),
    Interval(IntervalWindow),
}


impl Window {
    pub(crate) fn default() -> Self {
        Back(BackWindow::new(|t| t))
    }

    pub(crate) fn windowing(&self) -> Box<dyn Fn(Train) -> Train + Send + 'static> {
        match self {
            Back(w) => w.get_window(),
            Window::Interval(w) => w.get_window()
        }
    }

    pub(crate) fn parse(stencil: String) -> Self {
        Self::default()
    }
}

pub struct BackWindow {
    pub func: Option<Box<dyn Fn(Train) -> Train + Send + 'static>>,
}

impl BackWindow {
    pub(crate) fn get_window(&self) -> Box<dyn Fn(Train) -> Train + Send> {
        Box::new(|train: Train| -> Train{ train })
    }
}

impl BackWindow {
    pub fn new<F>(func: F) -> Self where F: Fn(Train) -> Train + Send + 'static {
        BackWindow { func: Some(Box::new(func)) }
    }
}

pub struct IntervalWindow {}

impl IntervalWindow {
    pub(crate) fn get_window(&self) -> Box<dyn Fn(Train) -> Train + Send> {
        Box::new(|train: Train| {
            return train;
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc::channel;

    use crate::processing::station::Station;
    use crate::processing::train::Train;
    use crate::value::Value;

    #[test]
    fn default_behavior() {
        let mut station = Station::new(0);


        let values = vec![Value::float(3.3), Value::int(3)];

        let (tx, rx) = channel();

        station.add_out(0, tx);
        station.operate();
        station.send(Train::new(values.clone()));

        let res = rx.recv();
        match res {
            Ok(t) => {
                assert_eq!(values.len(), t.values.len());
                for (i, value) in t.values.iter().enumerate() {
                    assert_eq!(*value, values[i]);
                    assert_ne!(Value::text(""), *value)
                }
            }
            Err(..) => assert!(false),
        }
    }
}