#[cfg(test)]
pub mod dummy {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::thread::{sleep, spawn};
    use std::time::Duration;

    use crate::algebra::{BoxedIterator, ValueIterator};
    use crate::analyse::{InputDerivable, OutputDerivationStrategy};
    use crate::processing::destination::Destination;
    use crate::processing::option::Configurable;
    use crate::processing::plan::{DestinationModel, SourceModel};
    use crate::processing::source::Source;
    use crate::processing::station::Command;
    use crate::processing::station::Command::{Ready, Stop};
    use value::train::Train;
    use crate::processing::transform::{Transform, Transformer};
    use crate::processing::Layout;
    use crate::ui::ConfigModel;
    use crate::util::{new_channel, new_id, Rx, Tx};
    use value::Value;
    use crossbeam::channel::{unbounded, Sender};
    use serde_json::Map;

    pub struct DummySource {
        id: usize,
        values: Option<Vec<Vec<Value>>>,
        delay: Duration,
        initial_delay: Duration,
        senders: Vec<Tx<Train>>,
    }

    impl DummySource {
        pub(crate) fn new(values: Vec<Vec<Value>>, delay: Duration) -> (Self, usize) {
            Self::new_with_delay(values, Duration::from_millis(0), delay)
        }

        pub(crate) fn new_with_delay(values: Vec<Vec<Value>>, initial_delay: Duration, delay: Duration) -> (Self, usize) {
            let id = new_id();
            (DummySource { id, values: Some(values), initial_delay, delay, senders: vec![] }, id)
        }
    }

    impl Configurable for DummySource {
        fn name(&self) -> String {
            String::from("Dummy")
        }

        fn options(&self) -> Map<String, serde_json::Value> {
            let mut options = serde_json::map::Map::new();
            if self.initial_delay.as_millis() != 0 {
                options.insert(String::from("initial_delay"), serde_json::Value::from(self.initial_delay.as_millis() as i64));
            }
            options.insert(String::from("delay"), serde_json::Value::from(self.delay.as_millis() as i64));

            options
        }
    }

    impl Source for DummySource {
        fn parse(options: Map<String, serde_json::Value>) -> Result<Self, String> {
            let delay = Duration::from_millis(options.get("delay").unwrap().as_u64().unwrap());

            let values = options.get("values").cloned().unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));

            let values: Value = serde_json::from_value(values).unwrap();

            let values = match values {
                Value::Array(a) => {
                    a.values.into_iter().map(|v| match v {
                        Value::Array(a) => {
                            a.values
                        }
                        _ => vec![]
                    }).collect()
                },
                _ => vec![]
            };

            let mut source = if options.contains_key("initial_delay") {
                let initial_delay = Duration::from_millis(options.get("initial_delay").unwrap().as_u64().unwrap());

                DummySource::new_with_delay(values, initial_delay, delay).0
            } else {
                DummySource::new(values, delay).0
            };

            match options.get("id") {
                None => {},
                Some(id) => {
                    source.id = id.as_u64().unwrap() as usize;
                }
            };
            Ok(source)
        }

        fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
            let id = self.id;

            let delay = self.delay;
            let initial_delay = self.initial_delay;
            let values = self.values.take().unwrap();
            let senders = self.senders.clone();
            let (tx, rx) = unbounded();

            let _handle = spawn(move || {
                control.send(Ready(id)).unwrap();

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
                        sender.send(Train::new(values.clone())).unwrap();
                    }
                    sleep(delay);
                }
                control.send(Stop(id)).unwrap();
            });
            tx
        }

        fn outs(&mut self) -> &mut Vec<Tx<Train>> {
            &mut self.senders
        }

        fn id(&self) -> usize {
            self.id
        }

        fn type_(&self) -> String {
            String::from("Dummy")
        }
        fn serialize(&self) -> SourceModel {
            SourceModel { type_name: String::from("Dummy"), id: self.id.to_string(), configs: HashMap::new() }
        }

        fn from(_configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
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
        id: usize,
        result_size: usize,
        pub(crate) results: Arc<Mutex<Vec<Train>>>,
        receiver: Option<Rx<Train>>,
        sender: Tx<Train>,
    }

    impl DummyDestination {
        pub(crate) fn new(result_size: usize) -> Self {
            let (tx, rx) = new_channel("dummy sender");
            DummyDestination {
                id: new_id(),
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
        fn name(&self) -> String {
            String::from("Dummy")
        }

        fn options(&self) -> Map<String, serde_json::Value> {
            let mut options = serde_json::map::Map::new();
            options.insert(String::from("result_size"), serde_json::Value::from(self.result_size));
            options
        }
    }

    impl Destination for DummyDestination {
        fn parse(options: Map<String, serde_json::Value>) -> Result<Self, String> {
            let result_size = options.get("result_size").unwrap().as_u64().unwrap() as usize;

            let mut destination = DummyDestination::new(result_size);

            if let Some(id) = options.get("id") {
                destination.id = id.as_i64().unwrap() as usize;
            }

            Ok(destination)
        }

        fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
            let id = self.id;
            let local = self.results();
            let receiver = self.receiver.take().unwrap();
            let result_amount = self.result_size;
            let (tx, rx) = unbounded();

            spawn(move || {
                control.send(Ready(id)).unwrap();
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
                control.send(Stop(id))
            });
            tx
        }

        fn get_in(&self) -> Tx<Train> {
            self.sender.clone()
        }

        fn id(&self) -> usize {
            self.id
        }

        fn type_(&self) -> String {
            String::from("Dummy")
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

    #[derive(Clone, Debug, PartialEq)]
    pub struct DummyDatabase{
        query: String,
        mapping: Option<HashMap<Value, Value>>,
    }

    impl DummyDatabase {
        pub(crate) fn new(query: String,) -> DummyDatabase {
            DummyDatabase { query, mapping: None }
        }

        pub fn add_mapping(&mut self, mapping: HashMap<Value, Value>) {
            self.mapping = Some(mapping);
        }
    }

    impl Configurable for DummyDatabase {
        fn name(&self) -> String {
            "DummyDb".to_string()
        }

        fn options(&self) -> Map<String, serde_json::Value> {
            Map::new()
        }
    }

    impl InputDerivable for DummyDatabase {
        fn derive_input_layout(&self) -> Option<Layout> {
            Some(Layout::default())
        }
    }

    impl Transformer for DummyDatabase {
        fn parse(options: Map<String, serde_json::Value>) -> Result<Self, String> {
            Ok(DummyDatabase::new(options.get("query").unwrap().to_string()))
        }

        fn optimize(&self, _transforms: HashMap<String, Transform>) -> Box<dyn ValueIterator<Item=Value> + Send> {
            Box::new(MappingIterator::new(self.mapping.clone().unwrap()))
        }

        fn get_output_derivation_strategy(&self) -> &OutputDerivationStrategy {
            &OutputDerivationStrategy::Undefined
        }
    }

    pub struct MappingIterator{
        mapping: HashMap<Value, Value>,
        values: Vec<Value>,
    }


    impl MappingIterator {
        pub fn new(mapping: HashMap<Value, Value>) -> MappingIterator {
            MappingIterator{mapping, values: Vec::new()}
        }

        pub(crate) fn get_value(&self, value: Value) -> Option<Value> {
            self.mapping.get(&value).cloned()
        }
    }

    impl Iterator for MappingIterator {
        type Item = Value;

        fn next(&mut self) -> Option<Self::Item> {
            if self.values.is_empty() {
                None
            } else {
                Some(self.values.remove(0))
            }
        }
    }

    impl ValueIterator for MappingIterator {
        fn dynamically_load(&mut self, train: Train) {
            if let Some(values) = train.values {
                for value in values {
                    let values = self.get_value(value);
                    if let Some(values) = values {
                        self.values.push(values);
                    }
                }
            }
        }

        fn clone(&self) -> BoxedIterator {
            Box::new(MappingIterator::new(self.mapping.clone()))
        }

        fn enrich(&mut self, _transforms: HashMap<String, Transform>) -> Option<BoxedIterator> {
            None
        }
    }
}




#[cfg(test)]
pub mod tests {
    use crate::processing::destination::Destination;
    use crate::processing::plan::Plan;
    use crate::processing::station::Command::{Ready, Stop};
    use crate::processing::station::Station;
    use crate::processing::tests::plan_test::dummy::{DummyDestination, DummySource};
    use crate::processing::transform::{FuncTransform, Transform};
    use crate::processing::Train;
    use crate::util::new_channel;
    use value::{Dict, Value};
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

        let (output_tx, output_rx) = new_channel("test");

        let mut second = Station::new(1);
        second.add_out(0, output_tx).unwrap();

        plan.build(0, first);
        plan.build(0, second);

        plan.operate().unwrap();

        input.send(Train::new(values.clone())).unwrap();

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

        let (output1_tx, output1_rx) = new_channel("test1");

        let (output2_tx, output2_rx) = new_channel("test2");

        let mut second = Station::new(1);
        second.add_out(0, output1_tx).unwrap();

        let mut third = Station::new(2);
        third.add_out(0, output2_tx).unwrap();

        plan.build(0, first);
        plan.build(0, second);
        plan.build_split(1, first_id).unwrap();
        plan.build(1, third);

        plan.operate().unwrap();

        input.send(Train::new(values.clone())).unwrap();

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

        plan.operate().unwrap();

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

    pub(crate) fn dump(value: &Vec<Vec<Value>>) -> String {
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
            id1 = source1,
            id2 = source4,
            values1 = dump(&values1.clone()),
            values4 = dump(&values4.clone()),
            destination = destination, len = 1);

        let mut plan = Plan::parse(&stencil).unwrap();
        let result = plan.get_result(destination);

        plan.operate().unwrap();

        // send ready
        plan.send_control(&source1, Ready(0));
        // source 1 ready + stop
        for _com in vec![Ready(1), Stop(1)] {
            match plan.control_receiver.1.recv() {
                Ok(_command) => {}
                Err(_) => panic!()
            }
        }

        sleep(Duration::from_millis(10));

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

        let (source, id) = DummySource::new(values, Duration::from_nanos(3));

        plan.build(0, station);

        plan.add_source(Box::new(source));
        plan.connect_in_out(0, id);

        let destination = DummyDestination::new(length);
        let dest_id = destination.id();
        plan.add_destination(Box::new(destination));
        plan.connect_in_out(0, dest_id);

        plan.operate().unwrap();
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
        values.push(vec![Value::from(Value::float(3.6))]);

        let source = 1;
        let destination = 4;

        let mut plan = Plan::parse(
            &format!("\
            0--1(f)--2\n\
            In\n\
            Dummy{{\"id\":{source}, \"delay\":1, \"values\":{values}}}:1\n\
            Out\n\
            Dummy{{\"id\":{destination}, \"result_size\":{size}}}:2",
                     values = dump(&values.clone()),
                     size = 1)
        ).unwrap();

        plan.operate().unwrap();

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
        let values = vec![vec![Value::float(3.6), Value::float(4.6)]];
        let res: Vec<Vec<Value>> = values.iter().cloned().map(|v| v.iter().cloned().map(|v| &v + &Value::int(1)).collect()).collect();
        let source = 1;
        let destination= 5;


        test_single_in_out("1{sql|SELECT * FROM $example($0)}", values.clone(), res.clone(), source, destination, true);
        test_single_in_out("1{sql|SELECT $example FROM $example($0)}", values, res, source, destination, true);
    }

    #[test]
    fn global_transformer_dict_test() {
        let values = vec![vec![Value::dict_from_kv("age", Value::float(3.6)), Value::dict_from_kv("age", Value::float(4.6))]];
        let res: Vec<Vec<Value>> = vec![vec![Value::float(4.6), Value::float(5.6)]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT * FROM $example($0.age)}", values.clone(), res.clone(), source, destination, true);
    }

    #[test]
    fn unwind_test() {
        let values = vec![vec![Value::array( vec![Value::float(3.6), Value::float(4.6)])]];
        let res: Vec<Vec<Value>> = vec![vec![Value::float(3.6), Value::float(4.6)]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT * FROM UNWIND($0)}", values.clone(), res.clone(), source, destination, true);
        test_single_in_out("1{sql|SELECT unwind FROM UNWIND($0)}", values.clone(), res.clone(), source, destination, true);
    }


    #[test]
    fn split_test() {
        let values = vec![vec!["Hey there".into(), "how are you".into()]];
        let res: Vec<Vec<Value>> = vec![vec![Value::array(vec!["Hey".into(), "there".into()]), Value::array(vec!["how".into(), "are".into(), "you".into()])]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT SPLIT($0, '\\s+') FROM $0}", values.clone(), res.clone(), source, destination, true);
    }

    #[test]
    fn nested_test() {
        let values = vec![vec!["Hey there".into(), 3.into()]];
        let res: Vec<Vec<Value>> = vec![vec!["Hey there".into(), 3.into()]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT * FROM (SELECT * FROM $0)}", values.clone(), res.clone(), source, destination, true);
    }

    #[test]
    fn wordcount_test() {
        let values = vec![vec!["Hey there".into(), "how are you".into()]];
        let res: Vec<Vec<Value>> = vec![vec!["Hey".into(), "there".into(),"how".into(), "are".into(), "you".into()]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT * FROM UNWIND(SELECT SPLIT($0, '\\s+') FROM $0)}", values.clone(), res.clone(), source, destination, false);
    }

    #[test]
    fn group_test() {
        let values = vec![vec!["Hey".into(), "Hallo".into(), "Hey".into()]];
        let res: Vec<Vec<Value>> = vec![vec![vec![2.into(), "Hey".into(), 7.into()].into(), vec![1.into(), "Hallo".into(), 6.into()].into()]];
        let source = 1;
        let destination = 5;

        test_single_in_out("1{sql|SELECT COUNT(*), $0, COUNT(*) + 5 FROM $0 GROUP BY $0}", values.clone(), res.clone(), source, destination, false);
    }


    #[test]
    fn wordcount_group_test() {
        let values = vec![vec!["Hey Hallo".into(), "Hey".into()]];
        let res: Vec<Vec<Value>> = vec![vec![vec!["Hey".into(), 2.into()].into(), vec!["Hallo".into(), 1.into()].into()]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT unwind, COUNT(*) FROM UNWIND(SELECT SPLIT($0, '\\s+') FROM $0) GROUP BY unwind}", values.clone(), res.clone(), source, destination, false);
    }

    #[test]
    fn dict_test() {
        let values = vec![vec![Value::float(3.6), Value::float(4.6)]];
        let res: Vec<Vec<Value>> = vec![vec![Value::dict_from_kv("key", Value::float(3.6)), Value::dict_from_kv("key", Value::float(4.6))]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT {'key': $0} FROM $0}", values.clone(), res.clone(), source, destination, true);
    }

    #[test]
    fn array_single_test() {
        let values = vec![vec![Value::float(4.6)]];
        let res: Vec<Vec<Value>> = vec![vec![Value::array(vec![Value::float(4.6)])]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT [$0] FROM $0}", values.clone(), res.clone(), source, destination, true);
    }

    #[test]
    fn array_multiple_test() {
        let values = vec![vec![Value::float(4.6)]];
        let res: Vec<Vec<Value>> = vec![vec![Value::array(vec![Value::float(4.6), Value::float(4.6)])]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT [$0,$0] FROM $0}", values.clone(), res.clone(), source, destination, true);
    }

    #[test]
    fn array_index_test() {
        let values = vec![vec![Value::array(vec![Value::float(1.0), Value::float(2.0)])]];
        let res: Vec<Vec<Value>> = vec![vec![Value::float(2.0)]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT $0.1 FROM $0}", values.clone(), res.clone(), source, destination, true);
    }

    #[test]
    fn array_swap_test() {
        let values = vec![vec![Value::array(vec![Value::float(1.0), Value::float(2.0)])]];
        let res: Vec<Vec<Value>> = vec![vec![Value::array(vec![Value::float(2.0), Value::float(1.0)])]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT [$0.1, $0.0] FROM $0}", values.clone(), res.clone(), source, destination, true);
    }

    #[test]
    fn dict_multi_key_test() {
        let values = vec![vec![Value::float(3.6), Value::float(4.6)]];
        let res: Vec<Vec<Value>> = vec![vec![
            Value::dict_from_pairs(vec![("key", Value::float(3.6)), ("key2", Value::float(3.6))]),
            Value::dict_from_pairs(vec![("key", Value::float(4.6)), ("key2", Value::float(4.6))])]];
        let source = 1;
        let destination = 5;


        test_single_in_out("1{sql|SELECT {'key': $0, 'key2': $0 } FROM $0}", values.clone(), res.clone(), source, destination, true);
    }

    fn test_single_in_out(query: &str, values: Vec<Vec<Value>>, res: Vec<Vec<Value>>, source: usize, destination: usize, ordered: bool) {
        let mut plan = Plan::parse(
            &format!("\
            0--{query}--2\n\
            \n\
            In\n\
            Dummy{{\"id\":{source}, \"delay\":1, \"values\":{values}}}:0\n\
            Out\n\
            Dummy{{\"id\":{destination}, \"result_size\":{size}}}:2\n\
            Transform\n\
            $example:Dummy{{}}",
                     query = query,
                     source = source,
                     values = dump(&values.clone()),
                     size = 1,
                     destination = destination)
        ).unwrap();

        // get result arc
        let result = plan.get_result(destination);

        plan.operate().unwrap();

        // start sources
        plan.send_control(&source, Ready(0));
        plan.send_control(&destination, Ready(0));

        // wait for startup else whe risk grabbing the lock too early
        for _command in 0..4 {
            assert!(vec![Ready(source), Ready(destination), Stop(source), Stop(destination)].contains(&plan.control_receiver.1.recv().unwrap()));
        }

        let lock = result.lock().unwrap();
        let mut train = lock.clone().pop().unwrap();
        drop(lock);

        let mut expected = res.get(0).unwrap().clone();

        assert_eq!(train.values.clone().unwrap().len(), expected.len());
        if ordered {
            for (i, value) in train.values.take().unwrap().into_iter().enumerate() {
                assert_eq!(expected.get(i).unwrap().clone(), value)
            }
        }else {
            for value in train.values.take().unwrap().into_iter() {
                let i = expected.iter().position(|x| x == &value);
                assert!(i.is_some());
                expected.remove(i.unwrap());
            }
        }
    }
}
