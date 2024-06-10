use crate::processing::train::Train;
use crate::value::Value;

pub struct Transform {
    pub func: Option<Box<dyn Fn(Train) -> Train + Send + 'static>>,
}

impl Transform {}

impl Transform {
    pub(crate) fn default() -> Self {
        Transform::new(Box::new(|f| f))
    }
    pub(crate) fn new<F>(func: F) -> Self where F: Fn(Train) -> Train + Send + 'static {
        Transform { func: Some(Box::new(func)) }
    }

    pub(crate) fn new_val<F>(func: F) -> Transform where F: Fn(Value) -> Value + Send + Clone + 'static   {
        Self::new(Box::new( move |t: Train| {
            let values = t.values.into_iter().map(func.clone()).collect();
            Train::new(values)
        }))
    }
}


#[cfg(test)]
mod tests {
    use std::sync::mpsc::channel;

    use crate::processing::station::Station;
    use crate::processing::train::Train;
    use crate::processing::transform::Transform;
    use crate::value::Value;

    #[test]
    fn transform_test() {
        let mut station = Station::new(0);

        station.transform(Transform::new_val(|x| &x + &Value::int(3)));

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
                    assert_eq!(*value, &values[i] + &Value::int(3));
                    assert_ne!(Value::text(""), *value)
                }
            }
            Err(..) => assert!(false),
        }
    }
}