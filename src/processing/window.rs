use crate::processing::train::Train;

pub struct Window {
    pub func: Option<Box<dyn Fn(Train) -> Train + Send + 'static>>,
}

impl Window {
    pub fn new<F>(func: F) -> Self where F: Fn(Train) -> Train + Send + 'static {
        Window { func: Some(Box::new(func)) }
    }

    pub(crate) fn default() -> Self {
        Window::new(|t| t)
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