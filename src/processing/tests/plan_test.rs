#[cfg(test)]
pub mod dummy {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::thread::{sleep, spawn};
    use std::time::Duration;

    use crate::processing::destination::Destination;
    use crate::processing::option::Configurable;
    use crate::processing::plan::{DestinationModel, SourceModel};
    use crate::processing::source::Source;
    use crate::processing::station::Command;
    use crate::processing::station::Command::{Ready, Stop};
    use crate::processing::train::Train;
    use crate::ui::ConfigModel;
    use crate::util::{new_channel, Rx, Tx, GLOBAL_ID};
    use crate::value::Value;
    use crossbeam::channel::{unbounded, Sender};
    use serde_json::Map;

    pub struct DummySource {
        id: i64,
        stop: i64,
        values: Option<Vec<Vec<Value>>>,
        delay: Duration,
        initial_delay: Duration,
        senders: Option<Vec<Tx<Train>>>,
    }

    impl DummySource {
        pub(crate) fn new(stop: i64, values: Vec<Vec<Value>>, delay: Duration) -> (Self, i64) {
            Self::new_with_delay(stop, values, Duration::from_millis(0), delay)
        }

        pub(crate) fn new_with_delay(stop: i64, values: Vec<Vec<Value>>, initial_delay: Duration, delay: Duration) -> (Self, i64) {
            let id = GLOBAL_ID.new_id();
            (DummySource { id, stop, values: Some(values), initial_delay, delay, senders: Some(vec![]) }, id)
        }
    }

    impl Configurable for DummySource {
        fn get_name(&self) -> String {
            String::from("Dummy")
        }

        fn get_options(&self) -> Map<String, serde_json::Value> {
            let mut options = serde_json::map::Map::new();
            if self.initial_delay.as_millis() != 0 {
                options.insert(String::from("initial_delay"), serde_json::Value::from(self.initial_delay.as_millis() as i64));
            }
            options.insert(String::from("delay"), serde_json::Value::from(self.delay.as_millis() as i64));

            options
        }
    }

    impl Source for DummySource {
        fn parse(stop: i64, options: Map<String, serde_json::Value>) -> Result<Self, String> {
            let delay = Duration::from_millis(options.get("delay").unwrap().as_u64().unwrap());

            let values = options.get("values").cloned().unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));

            let values: Value = serde_json::from_value(values).unwrap();

            let values = match values {
                Value::Array(a) => {
                    a.0.into_iter().map(|v| match v {
                        Value::Array(a) => {
                            a.0
                        }
                        _ => vec![]
                    }).collect()
                },
                _ => vec![]
            };

            let mut source = if options.contains_key("initial_delay") {
                let initial_delay = Duration::from_millis(options.get("initial_delay").unwrap().as_u64().unwrap());

                DummySource::new_with_delay(stop, values, initial_delay, delay).0
            } else {
                DummySource::new(stop, values, delay).0
            };

            match options.get("id") {
                None => {},
                Some(id) => {
                    source.id = id.as_i64().unwrap();
                }
            };
            Ok(source)
        }

        fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
            let stop = self.stop;

            let delay = self.delay;
            let initial_delay = self.initial_delay;
            let values = self.values.take().unwrap();
            let senders = self.senders.take().unwrap();
            let (tx, rx) = unbounded();

            let _handle = spawn(move || {
                control.send(Ready(stop)).unwrap();

                // wait for ready from callee
                match rx.recv() {
                    Ok(command) => {
                        match command {
                            Ready(_id) => {}
                            _ => panic!()
                        }
                    }
                    _ => panic!()
                }
                sleep(initial_delay);


                for values in &values {
                    for sender in &senders {
                        sender.send(Train::new(0, values.clone())).unwrap();
                    }
                    sleep(delay);
                }
                control.send(Stop(stop)).unwrap();
            });
            tx
        }


        fn add_out(&mut self, _id: i64, out: Tx<Train>) {
            self.senders.as_mut().unwrap_or(&mut vec![]).push(out)
        }

        fn get_stop(&self) -> i64 {
            self.stop
        }

        fn get_id(&self) -> i64 {
            self.id
        }

        fn serialize(&self) -> SourceModel {
            SourceModel { type_name: String::from("Dummy"), id: self.id.to_string(), configs: HashMap::new() }
        }

        fn from(_stop_id: i64, _configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
        where
            Self: Sized,
        {
            Err(String::from("This source does not allow for modifications."))
        }

        fn serialize_default() -> Result<SourceModel, ()> {
            Err(())
        }
    }

    pub struct DummyDestination {
        id: i64,
        stop: i64,
        result_size: usize,
        pub(crate) results: Arc<Mutex<Vec<Train>>>,
        receiver: Option<Rx<Train>>,
        sender: Tx<Train>,
    }

    impl DummyDestination {
        pub(crate) fn new(stop: i64, result_size: usize) -> Self {
            let (tx, _num, rx) = new_channel();
            DummyDestination {
                id: GLOBAL_ID.new_id(),
                stop,
                result_size,
                results: Arc::new(Mutex::new(vec![])),
                receiver: Some(rx),
                sender: tx,
            }
        }

        pub(crate) fn results(&self) -> Arc<Mutex<Vec<Train>>> {
            Arc::clone(&self.results)
        }
    }

    impl Configurable for DummyDestination {
        fn get_name(&self) -> String {
            String::from("Dummy")
        }

        fn get_options(&self) -> Map<String, serde_json::Value> {
            let mut options = serde_json::map::Map::new();
            options.insert(String::from("result_size"), serde_json::Value::from(self.result_size));
            options
        }
    }

    impl Destination for DummyDestination {
        fn parse(stop: i64, options: Map<String, serde_json::Value>) -> Result<Self, String> {
            let result_size = options.get("result_size").unwrap().as_u64().unwrap() as usize;

            let mut destination = DummyDestination::new(stop, result_size);

            if let Some(id) = options.get("id") {
                destination.id = id.as_i64().unwrap();
            }

            Ok(destination)
        }

        fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
            let stop = self.stop;
            let local = self.results();
            let receiver = self.receiver.take().unwrap();
            let result_amount = self.result_size as usize;
            let (tx, rx) = unbounded();

            spawn(move || {
                control.send(Ready(stop)).unwrap();
                let mut shared = local.lock().unwrap();
                loop {
                    match rx.try_recv() {
                        Ok(command) => match command {
                            Stop(_) => break,
                            _ => {}
                        },
                        _ => {}
                    }
                    match receiver.try_recv() {
                        Ok(train) => {
                            shared.push(train);
                            if shared.len() == result_amount {
                                break;
                            }
                        }
                        _ => sleep(Duration::from_nanos(100))
                    }
                }
                drop(shared);
                control.send(Stop(stop))
            });
            tx
        }

        fn get_in(&self) -> Tx<Train> {
            self.sender.clone()
        }

        fn get_stop(&self) -> i64 {
            self.stop
        }

        fn get_id(&self) -> i64 {
            self.id
        }

        fn serialize(&self) -> DestinationModel {
            DestinationModel { type_name: String::from("Dummy"), id: self.id.to_string(), configs: HashMap::new() }
        }

        fn serialize_default() -> Option<DestinationModel>
        where
            Self: Sized,
        {
            None
        }

        fn get_result_handle(&self) -> Arc<Mutex<Vec<Train>>> {
            self.results.clone()
        }

    }
}


#[cfg(test)]
pub mod tests {
    use crate::processing::plan::Plan;
    use crate::processing::station::Command::{Ready, Stop};
    use crate::processing::station::Station;
    use crate::processing::tests::plan_test::dummy::{DummyDestination, DummySource};
    use crate::processing::transform::{FuncTransform, Transform};
    use crate::processing::Train;
    use crate::util::new_channel;
    use crate::value::{Dict, Value};
    use std::any::Any;
    use std::thread::sleep;
    use std::time::{Duration, SystemTime};
    use std::vec;

    pub fn dict_values(values: Vec<Value>) -> Vec<Value> {
        let mut dicts = vec![];
        for value in values {
            dicts.push(Value::Dict(Dict::from(value)));
        }
        dicts
    }

    #[test]
    fn station_plan_train() {
        let values = vec![Value::int(3).into(), Value::text("test").into()];

        let mut plan = Plan::default();
        let mut first = Station::new(0);
        let input = first.get_in();

        let (output_tx, _nums, output_rx) = new_channel();

        let mut second = Station::new(1);
        second.add_out(0, output_tx).unwrap();

        plan.build(0, first);
        plan.build(0, second);

        plan.operate();

        input.send(Train::new(0, values.clone())).unwrap();

        let mut res = output_rx.recv().unwrap();
        assert_eq!(res.values.clone().unwrap(), values);
        assert_ne!(res.values.take().unwrap(), vec![Value::null().into()]);

        assert!(output_rx.try_recv().is_err());


        drop(input); // close the channel
        plan.halt()
    }

    #[test]
    fn station_plan_split_train() {
        let values = dict_values(vec![3.into(), "test".into(), true.into(), Value::null()]);

        let mut plan = Plan::default();
        let mut first = Station::new(0);
        let first_id = first.stop;
        let input = first.get_in();

        let (output1_tx, _num, output1_rx) = new_channel();

        let (output2_tx, _num, output2_rx) = new_channel();

        let mut second = Station::new(1);
        second.add_out(0, output1_tx).unwrap();

        let mut third = Station::new(2);
        third.add_out(0, output2_tx).unwrap();

        plan.build(0, first);
        plan.build(0, second);
        plan.build_split(1, first_id).unwrap();
        plan.build(1, third);

        plan.operate();

        input.send(Train::new(0, values.clone())).unwrap();

        let mut res = output1_rx.recv().unwrap();
        assert_eq!(res.values.clone().unwrap(), values);
        assert_ne!(res.values.take().unwrap(), vec![Value::null().into()]);

        assert!(output1_rx.try_recv().is_err());

        let mut res = output2_rx.recv().unwrap();
        assert_eq!(res.values.clone().unwrap(), values);
        assert_ne!(res.values.take().unwrap(), vec![Value::null().into()]);

        assert!(output2_rx.try_recv().is_err());


        drop(input); // close the channel
        plan.halt()
    }


    #[test]
    fn sql_parse_transform() {
        let values = vec![vec![3.into(), "test".into(), true.into(), Value::null()]];
        let id = 3;
        let destination = 4;
        let stencil = format!("\
        3{{sql|Select * From $0}}\n\
        In\n\
        Dummy{{\"id\": {}, \"delay\":{},\"values\":{}}}:3\n\
        Out\n\
        Dummy{{\"id\": {},\"result_size\":{}}}:3", id, 3, dump(&values), destination, values.len());

        let mut plan = Plan::parse(&stencil).unwrap();

        let clone = plan.get_result(destination);


        plan.operate();

        // start dummy source
        plan.send_control(&id, Ready(3));

        // source ready + stop, destination ready + stop
        for _command in vec![Ready(3), Stop(3), Ready(3), Stop(3)] {
            plan.control_receiver.1.recv().unwrap();
        }


        let results = clone.lock().unwrap();
        for mut train in results.clone() {
            assert_eq!(train.values.take().unwrap(), *values.get(0).unwrap())
        }
    }

    fn dump(value: &Vec<Vec<Value>>) -> String {
        let values: Value = value.iter().cloned().map(|v| v.into()).collect::<Vec<_>>().into();
        serde_json::to_string(&values).unwrap()
    }

    #[test]
    fn sql_parse_block_one() {
        let source1 = 1;
        let source4 = 4;
        let destination = 5;
        let values1 = vec![vec![Value::from(Dict::from(Value::from(3.3)))], vec![Value::from(Dict::from(Value::from(3.1)))]];
        let values4 = vec![vec![Value::from(Dict::from(Value::from(3)))]];
        let stencil = format!(
            "1-|2--3\n\
            4--2\n\
            \n\
            In\n\
            Dummy{{\"id\": {id1}, \"delay\":1, \"values\":{values1}}}:1\n\
            Dummy{{\"id\": {id2}, \"delay\":1, \"values\":{values4}}}:4\n\
            Out\n\
            Dummy{{\"id\":{destination}, \"result_size\":{len}}}:3",
            id1 = source1, id2 = source4, values1 = dump(&values1.clone()), values4 = dump(&values4.clone()), destination = destination, len = 1);

        let mut plan = Plan::parse(&stencil).unwrap();
        let result = plan.get_result(destination);

        plan.operate();

        // send ready
        plan.send_control(&source1, Ready(0));
        // source 1 ready + stop
        for _com in vec![Ready(1), Stop(1)] {
            match plan.control_receiver.1.recv() {
                Ok(_command) => {}
                Err(_) => panic!()
            }
        }

        sleep(Duration::from_millis(5));

        plan.send_control(&source4, Ready(4));
        // source 1 ready + stop
        for _com in vec![Ready(4), Stop(4), Ready(3), Stop(3)] {
            match plan.control_receiver.1.recv() {
                Ok(_command) => {}
                Err(_) => panic!()
            }
        }

        let mut res = vec![];

        values1.into_iter().for_each(|mut values| res.append(&mut values));
        values4.into_iter().for_each(|mut values| res.append(&mut values));

        let lock = result.lock().unwrap();
        let mut train = lock.clone().pop().unwrap();
        drop(lock);


        assert_eq!(train.values.clone().unwrap().len(), res.len());
        for (_i, value) in train.values.take().unwrap().into_iter().enumerate() {
            assert!(res.contains(&value))
        }
    }

    #[test]
    fn divide_workload() {
        let mut station = Station::new(0);

        station.set_transform(Transform::Func(FuncTransform::new_boxed(|_num, train| {
            sleep(Duration::from_millis(10));
            train
        })));

        let mut values = vec![];

        let numbers = 0..1_000;
        let length = numbers.len();

        for _num in numbers {
            values.push(dict_values(vec![Value::int(3)]));
        }

        let mut plan = Plan::new(0);

        let (source, id) = DummySource::new(0, values, Duration::from_nanos(3));

        plan.build(0, station);

        plan.add_source(0, Box::new(source));

        let destination = DummyDestination::new(0, length);
        plan.add_destination(0, Box::new(destination));

        plan.operate();
        let now = SystemTime::now();
        plan.send_control(&id, Ready(0));
        plan.clone_platform(0);
        plan.clone_platform(0);
        plan.clone_platform(0);

        // source 1 ready + stop, each platform ready, destination ready (+ stop only after stopped)
        for _com in vec![Ready(1), Stop(1), Ready(0), Ready(0), Ready(0), Ready(0), Ready(0)] {
            match plan.control_receiver.1.recv() {
                Ok(_command) => {}
                Err(_) => panic!()
            }
        }

        println!("time: {} millis", now.elapsed().unwrap().as_millis())
    }


    #[test]
    fn full_test() {
        let mut values = vec![];

        let hello = Value::Dict(Dict::from_json(r#"{"msg": "hello", "$topic": ["command"]}"#));
        values.push(vec![hello]);
        values.push(vec![Value::from(Dict::from(Value::float(3.6)))]);

        let source = 1;
        let destination = 4;

        let mut plan = Plan::parse(
            &format!("\
            0--1($:f)--2\n\
            In\n\
            Dummy{{\"id\":{source}, \"delay\":1, \"values\":{values}}}:1\n\
            Out\n\
            Dummy{{\"id\":{destination}, \"result_size\":{size}}}:2", values = dump(&values.clone()), size = 1)
        ).unwrap();

        plan.operate();

        let result = plan.get_result(destination);
        plan.send_control(&source, Ready(0));

        for command in [Ready(0), Stop(0), Ready(2), Stop(2)] {
            assert_eq!(command.type_id(), plan.control_receiver.1.recv().unwrap().type_id());
        }
        let lock = result.lock().unwrap();
        assert_eq!(lock.len(), 1)
    }

    #[test]
    fn global_transformer_test() {
        let mut values = vec![vec![Value::float(3.6), Value::float(4.6)]];
        let res: Vec<Vec<Value>> = values.iter().cloned().map(|v| v.iter().cloned().map(|v| &v + &Value::int(1)).collect()).collect();
        let source = 1;
        let destination= 5;

        let mut plan = Plan::parse(
            &format!("\
            0--1{{sql|SELECT * FROM $example($0)}}--2\n\
            \n\
            In\n\
            Dummy{{\"id\":{source}, \"delay\":1, \"values\":{values}}}:1\n\
            Out\n\
            Dummy{{\"id\":{destination}, \"result_size\":{size}}}:2\n\
            Transform\n\
            $example:Dummy{{}}", source = source, values = dump(&values.clone()), size = 1, destination = destination)
        ).unwrap();

        // get result arc
        let result = plan.get_result(destination);

        plan.operate();


        // start sources
        plan.send_control(&source, Ready(0));
        plan.send_control(&destination, Ready(0));

        // wait for startup else whe risk grabbing the lock too early
        for command in [Ready(0), Ready(0)] {
            plan.control_receiver.1.recv().unwrap();
        }

        let lock = result.lock().unwrap();
        let mut train = lock.clone().pop().unwrap();
        drop(lock);

        assert_eq!(train.values.clone().unwrap().len(), res.get(0).unwrap().len());
        for (i, value) in train.values.take().unwrap().into_iter().enumerate() {
            assert_eq!(res.get(0).unwrap().get(i).unwrap().clone(), value)
        }
    }
}
