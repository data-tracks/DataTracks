#[cfg(test)]
mod dummy {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::thread::{sleep, spawn};
    use std::time::Duration;

    use crossbeam::channel::{unbounded, Sender};

    use crate::processing::destination::Destination;
    use crate::processing::plan::{DestinationModel, SourceModel};
    use crate::processing::source::Source;
    use crate::processing::station::Command;
    use crate::processing::station::Command::{Ready, Stop};
    use crate::processing::train::Train;
    use crate::ui::ConfigModel;
    use crate::util::{new_channel, Rx, Tx, GLOBAL_ID};
    use crate::value::Value;

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


    impl Source for DummySource {
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

    pub(crate) struct DummyDestination {
        id: i64,
        stop: i64,
        result_amount: usize,
        pub(crate) results: Arc<Mutex<Vec<Train>>>,
        receiver: Option<Rx<Train>>,
        sender: Tx<Train>,
    }

    impl DummyDestination {
        pub(crate) fn new(stop: i64, wait_result: usize) -> Self {
            let (tx, _num, rx) = new_channel();
            DummyDestination {
                id: GLOBAL_ID.new_id(),
                stop,
                result_amount: wait_result,
                results: Arc::new(Mutex::new(vec![])),
                receiver: Some(rx),
                sender: tx,
            }
        }

        pub(crate) fn results(&self) -> Arc<Mutex<Vec<Train>>> {
            Arc::clone(&self.results)
        }
    }

    impl Destination for DummyDestination {
        fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
            let stop = self.stop;
            let local = self.results();
            let receiver = self.receiver.take().unwrap();
            let result_amount = self.result_amount as usize;
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
    }
}


#[cfg(test)]
pub mod tests {
    use std::any::Any;
    use std::thread::sleep;
    use std::time::{Duration, SystemTime};
    use std::vec;

    use crate::processing::destination::Destination;
    use crate::processing::plan::Plan;
    use crate::processing::station::Command::{Ready, Stop};
    use crate::processing::station::Station;
    use crate::processing::tests::plan_test::dummy::{DummyDestination, DummySource};
    use crate::processing::transform::{FuncTransform, Transform};
    use crate::processing::Train;
    use crate::util::new_channel;
    use crate::value::{Dict, Value};

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
        let values = vec![dict_values(vec![3.into(), "test".into(), true.into(), Value::null()])];
        let stencil = "3{sql|Select * From $0}";

        let mut plan = Plan::parse(stencil);

        let (source, id) = DummySource::new(3, values.clone(), Duration::from_millis(3));

        let destination = DummyDestination::new(3, values.len());
        let clone = destination.results();

        plan.add_source(3, Box::new(source));
        plan.add_destination(3, Box::new(destination));


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

    #[test]
    fn sql_parse_block_one() {
        let stencil = "1-|2-3\n4-2";

        let mut plan = Plan::parse(stencil);
        let values1 = vec![vec![Value::from(Dict::from(Value::from(3.3)))], vec![Value::from(Dict::from(Value::from(3.1)))]];
        let (source1, id1) = DummySource::new(1, values1.clone(), Duration::from_millis(1));

        let values4 = vec![vec![Value::from(Dict::from(Value::from(3)))]];
        let (source4, id4) = DummySource::new(4, values4.clone(), Duration::from_millis(1));

        let destination = DummyDestination::new(3, 1);
        let _id3 = &destination.get_id();
        let clone = destination.results();

        plan.add_source(1, Box::new(source1));
        plan.add_source(4, Box::new(source4));
        plan.add_destination(3, Box::new(destination));

        plan.operate();

        // send ready
        plan.send_control(&id1, Ready(0));
        // source 1 ready + stop
        for _com in vec![Ready(1), Stop(1)] {
            match plan.control_receiver.1.recv() {
                Ok(_command) => {}
                Err(_) => panic!()
            }
        }

        sleep(Duration::from_millis(5));

        plan.send_control(&id4, Ready(4));
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

        let lock = clone.lock().unwrap();
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
        let mut plan = Plan::parse("0-1($:f)-2");

        let mut values = vec![];

        let hello = Value::Dict(Dict::from_json(r#"{"msg": "hello", "$topic": ["command"]}"#));
        values.push(vec![hello]);
        values.push(vec![Value::from(Dict::from(Value::float(3.6)))]);

        let (source, id) = DummySource::new(0, values, Duration::from_millis(1));

        plan.add_source(0, Box::new(source));

        let destination = DummyDestination::new(2, 1);
        let result = destination.results();
        plan.add_destination(2, Box::new(destination));

        plan.operate();

        plan.send_control(&id, Ready(0));

        for command in [Ready(0), Stop(0), Ready(2), Stop(2)] {
            assert_eq!(command.type_id(), plan.control_receiver.1.recv().unwrap().type_id());
        }
        let lock = result.lock().unwrap();
        assert_eq!(lock.len(), 1)
    }

    #[test]
    fn advertise_name_join_test() {
        let mut plan = Plan::parse("0-1{SELECT $0.id FROM $0, $3 WHERE $0.name = $3.name}-2\n3{sql|SELECT company.name, company.id FROM company}<1");

        let mut values = vec![];

        let hello = Value::Dict(Dict::from_json(r#"{"msg": "hello", "$topic": ["command"]}"#));
        values.push(vec![hello]);
        values.push(vec![Value::from(Dict::from(Value::float(3.6)))]);

        let (source, id) = DummySource::new(0, values, Duration::from_millis(1));

        plan.add_source(0, Box::new(source));

        let destination = DummyDestination::new(2, 1);
        let result = destination.results();
        plan.add_destination(2, Box::new(destination));

        plan.operate();

        plan.send_control(&id, Ready(0));

        for command in [Ready(0), Stop(0), Ready(2), Stop(2)] {
            assert_eq!(command.type_id(), plan.control_receiver.1.recv().unwrap().type_id());
        }
        let lock = result.lock().unwrap();
        assert_eq!(lock.len(), 1)
    }
}

