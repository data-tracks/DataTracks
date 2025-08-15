use std::collections::HashMap;
use track_rails::message_generated::protocol::StationArgs;

use crate::processing::layout::Layout;
use crate::processing::plan::PlanStage;
use crate::processing::platform::Platform;
use crate::processing::sender::Sender;
use crate::processing::transform::Transforms;
use crate::processing::watermark::WatermarkStrategy;
use crate::processing::window::Window;
use crate::util::{HybridThreadPool, TriggerType};
use crate::util::{Rx, Tx};
use crate::util::{new_channel, new_id};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use tracing::debug;
use track_rails::message_generated::protocol::Station as FlatStation;
use value::train::Train;

#[derive(Clone)]
pub struct Station {
    pub id: usize,
    pub stop: usize,
    pub incoming: (Tx<Train>, Rx<Train>),
    pub outgoing: Sender,
    pub window: Window,
    pub transform: Option<Transforms>,
    pub trigger: TriggerType,
    pub block: Vec<usize>,
    pub inputs: Vec<usize>,
    pub layout: Layout,
    pub watermark_strategy: WatermarkStrategy,
}

impl Default for Station {
    fn default() -> Self {
        Self::new(usize::MAX)
    }
}

impl Station {
    pub(crate) fn new(stop: usize) -> Self {
        let incoming = new_channel(format!("Incoming {stop}"), false);
        Station {
            id: new_id(),
            stop,
            incoming: (incoming.0, incoming.1),
            outgoing: Sender::default(),
            window: Window::default(),
            transform: None,
            trigger: TriggerType::Element,
            block: vec![],
            inputs: vec![],
            layout: Layout::default(),
            watermark_strategy: Default::default(),
        }
    }

    pub fn flatternize<'a>(
        &self,
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<FlatStation<'a>> {
        let blocks =
            builder.create_vector(&self.block.iter().map(|b| *b as u64).collect::<Vec<_>>());
        let inputs =
            builder.create_vector(&self.inputs.iter().map(|b| *b as u64).collect::<Vec<_>>());
        let transform = self
            .transform
            .clone()
            .map(|t| t.flatternize(builder))
            .flatten();
        FlatStation::create(
            builder,
            &StationArgs {
                id: self.id as u64,
                stop: self.stop as u64,
                transform,
                block: Some(blocks),
                inputs: Some(inputs),
            },
        )
    }

    // |1 or <1 or -1
    pub(crate) fn parse(stencil: String, last: Option<usize>) -> Result<Self, String> {
        let mut stencil = stencil;
        if stencil.starts_with('-') {
            stencil = stencil[1..].to_string()
        }

        let mut temp = String::default();
        let mut is_text = false;
        let mut stages = vec![];
        let mut current_stage = Some(PlanStage::Num);
        let mut counter = 0;

        for char in stencil.chars() {
            if char != '"' && is_text {
                temp.push(char);
                continue;
            }

            // can we end number?
            if let Some(_stage @ PlanStage::Num) = current_stage {
                if PlanStage::is_open(char) {
                    stages.push((PlanStage::Num, temp.clone()));
                    temp = String::default();
                    current_stage = None;
                }
            }

            match char {
                '"' => {
                    is_text = !is_text;
                    if current_stage == Some(PlanStage::Transform) {
                        temp.push('"');
                    }
                }
                PlanStage::WINDOW_OPEN | PlanStage::TRANSFORM_OPEN | PlanStage::LAYOUT_OPEN => {
                    if current_stage.is_some() {
                        temp.push(char);
                        counter += 1;
                        continue;
                    }
                    current_stage = Some(match char {
                        PlanStage::LAYOUT_OPEN => PlanStage::Layout,
                        PlanStage::TRANSFORM_OPEN => PlanStage::Transform,
                        PlanStage::WINDOW_OPEN => PlanStage::Window,
                        _ => current_stage.unwrap(),
                    })
                }
                c => {
                    if let Some(stage) = current_stage {
                        if counter == 0
                            && ((c == PlanStage::LAYOUT_CLOSE && stage == PlanStage::Layout)
                                || (c == PlanStage::TRANSFORM_CLOSE
                                    && stage == PlanStage::Transform)
                                || (c == PlanStage::WINDOW_CLOSE && stage == PlanStage::Window))
                        {
                            stages.push((stage, temp.clone()));
                            temp = String::default();
                            current_stage = None;
                            continue;
                        } else if c == PlanStage::LAYOUT_CLOSE
                            || c == PlanStage::TRANSFORM_CLOSE
                            || c == PlanStage::WINDOW_CLOSE
                        {
                            counter -= 1;
                        }
                    }
                    temp.push(char);
                }
            }
        }
        if !temp.is_empty() {
            if let Some(stage @ PlanStage::Num) = current_stage {
                stages.push((stage, temp))
            } else {
                panic!("Not finished parsing")
            }
        }

        Self::parse_parts(last, stages)
    }

    pub(crate) fn parse_parts(
        last: Option<usize>,
        parts: Vec<(PlanStage, String)>,
    ) -> Result<Self, String> {
        let mut station: Station = Station::default();
        for stage in parts {
            match stage.0 {
                PlanStage::Window => station.set_window(Window::parse(stage.1)?),
                PlanStage::Transform => station.set_transform(Transforms::try_from(stage.1)?),
                PlanStage::Layout => station.add_explicit_layout(Layout::parse(&stage.1)),
                PlanStage::Num => {
                    let mut num = stage.1;
                    // test for blocks
                    if num.starts_with('|') {
                        station.add_block(last.unwrap_or(0));
                        num.remove(0);
                    }
                    station.set_stop(
                        num.parse()
                            .map_err(|err| format!("Could not parse stop number: {err}"))?,
                    )
                }
            }
        }
        Ok(station)
    }

    pub(crate) fn add_insert(&mut self, input: usize) {
        self.inputs.push(input);
    }

    pub(crate) fn merge(&mut self, mut other: Station) {
        self.block.append(other.block.as_mut());
        other.transform.into_iter().for_each(|transform| {
            self.transform = Some(transform.clone());
        })
    }

    pub fn derive_output_layout(&self, inputs: HashMap<String, Layout>) -> Layout {
        if let Some(transform) = self.transform.clone() {
            transform.derive_output_layout(inputs).unwrap_or_default()
        } else {
            inputs.values().next().cloned().unwrap_or_default()
        }
    }

    pub fn derive_input_layout(&self) -> Layout {
        self.clone()
            .transform
            .map(|t| t.derive_input_layout().unwrap_or_default())
            .unwrap_or_default()
    }

    pub(crate) fn set_stop(&mut self, stop: usize) {
        self.stop = stop
    }

    pub(crate) fn set_window(&mut self, window: Window) {
        self.window = window;
    }

    pub(crate) fn set_transform(&mut self, transform: Transforms) {
        self.transform = Some(transform);
    }

    pub(crate) fn add_block(&mut self, line: usize) {
        self.block.push(line);
    }

    pub(crate) fn add_out(&mut self, id: usize, out: Tx<Train>) -> Result<(), String> {
        self.outgoing.add(id, out);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fake_receive(&mut self, train: Train) -> Result<(), String> {
        self.incoming.0.send(train)
    }

    pub fn dump(&self, already_dumped: bool) -> String {
        let mut dump = "".to_string();

        dump += &self.stop.to_string();
        dump += &self.window.dump();
        if !already_dumped {
            if let Some(transform) = self.transform.clone() {
                dump += &transform.dump(false);
            }
        }

        dump
    }

    pub(crate) fn get_in(&mut self) -> Tx<Train> {
        self.incoming.0.clone()
    }

    #[cfg(test)]
    pub(crate) fn operate_test(
        &mut self,
        transforms: HashMap<String, Transforms>,
    ) -> (usize, HybridThreadPool) {
        let pool = HybridThreadPool::default();
        let id = self.operate(transforms, pool.clone()).unwrap();
        (id, pool)
    }

    pub(crate) fn operate(
        &mut self,
        transforms: HashMap<String, Transforms>,
        pool: HybridThreadPool,
    ) -> Result<usize, String> {
        let mut platform = Platform::new(self, transforms);
        let stop = self.stop;

        pool.execute_sync(format!("Station {}", self.id), move |args| {
            debug!("Starting station {stop}");
            platform.operate(args)
        })
    }

    fn add_explicit_layout(&mut self, layout: Layout) {
        self.layout = layout;
    }
}

#[cfg(test)]
pub mod tests {
    use crate::processing::window::Window;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use crate::processing::plan::Plan;
    use crate::processing::station::Station;
    use crate::processing::transform::{FuncTransform, Transforms};
    #[cfg(test)]
    pub use crate::tests::dict_values;
    use crate::util::{TimeUnit, TriggerType};

    use threading::channel::{Rx, Tx, new_channel};
    use threading::command::Command::{Okay, Ready, Stop, Threshold};
    use tracing_test::traced_test;
    use value::train::Train;
    use value::{Dict, Time, Value};

    #[test]
    #[traced_test]
    fn start_stop_test() {
        let mut station = Station::new(0);

        let mut values = dict_values(vec![
            Value::text("test"),
            Value::bool(true),
            Value::float(3.3),
            Value::null(),
        ]);

        for x in 0..10 {
            values.push(Value::Dict(Dict::from(Value::int(x))))
        }

        let (tx, rx) = new_channel("test", false);

        station.add_out(0, tx).unwrap();
        let (_, pool) = station.operate_test(HashMap::new());
        station
            .fake_receive(Train::new_values(values.clone(), 0, 0))
            .unwrap();

        let res = rx.recv();
        match res {
            Ok(t) => {
                for (i, value) in t.into_values().iter().enumerate() {
                    assert_eq!(value, &values[i]);
                    assert_ne!(&Value::text(""), value.as_dict().unwrap().get("$").unwrap())
                }
            }
            Err(..) => unreachable!(),
        }
        drop(pool)
    }

    #[test]
    fn station_two_train() {
        let values = vec![Value::Dict(Dict::from(Value::int(3)))];

        let mut first = Station::new(0);
        let input = first.get_in();

        let (output_tx, output_rx) = new_channel("test", false);

        let mut second = Station::new(1);
        second.add_out(0, output_tx).unwrap();

        let tx = second.get_in();
        first.add_out(1, tx).unwrap();

        let (id, pool) = first.operate_test(HashMap::new());
        let id_2 = second.operate(HashMap::new(), pool.clone()).unwrap();

        input.send(Train::new_values(values.clone(), 0, 0)).unwrap();

        let res = output_rx.recv().unwrap();
        assert_eq!(res.clone().into_values(), values);
        assert_ne!(res.into_values().clone(), vec![Value::null().into()]);

        assert!(output_rx.try_recv().is_err());

        drop(input); // close the channel

        pool.stop(&id).unwrap();
        pool.stop(&id_2).unwrap();
    }

    #[test]
    fn sql_parse_block() {
        let stencil = "1-|3{sql|Select * From $1}";

        let plan = Plan::parse(stencil).unwrap();

        let station = plan.stations.get(&3).unwrap();

        assert!(station.block.contains(&1));
    }

    #[test]
    fn sql_parse_layout() {
        let stencils = vec![
            "1()",                      // no fixed
            "1(f)",                     // fixed unnamed
            "1({temperature:f})",       // fixed float
            "1({name:t})",              // fixed text
            "1({temp:f, age: i})",      // fixed multiple
            "1({address:{number: i}})", // fixed nested document
            "1({locations:[i]})",       // fixed array
        ];

        for stencil in stencils {
            let plan = Plan::parse(stencil).unwrap();

            let _station = plan.stations.get(&1).unwrap();
        }
    }

    #[test]
    fn too_high() {
        let (mut station, train_sender, _rx) = create_test_station(10);

        let (id, pool) = station.operate_test(HashMap::new());
        pool.send_control(&id, Threshold(3)).unwrap();

        for _ in 0..1_000 {
            train_sender
                .send(Train::new_values(dict_values(vec![Value::int(3)]), 0, 0))
                .unwrap();
        }

        let receiver = pool.control_receiver();

        // the station should start, the threshold should be reached and after some time be balanced
        for state in vec![Ready(0), Threshold(0), Okay(0)] {
            assert_eq!(state, receiver.recv().unwrap())
        }
    }

    // ignorde for now
    fn too_high_two() {
        let (mut station, train_sender, _rx) = create_test_station(100);

        let (id, pool) = station.operate_test(HashMap::new());
        pool.send_control(&id, Threshold(3)).unwrap();

        let (id, pool) = station.operate_test(HashMap::new());
        pool.send_control(&id, Threshold(3)).unwrap();

        // second platform
        let _ = station.operate(HashMap::new(), pool.clone()).unwrap();

        for _ in 0..1_000 {
            train_sender
                .send(Train::new_values(dict_values(vec![Value::int(3)]), 0, 0))
                .unwrap();
        }

        let receiver = pool.control_receiver();
        // the station should open a platform, the station starts another platform,  the threshold should be reached and after some time be balanced
        for state in vec![Ready(0), Ready(0), Threshold(0), Okay(0)] {
            assert_eq!(state, receiver.recv().unwrap())
        }
    }

    #[test]
    fn remove_during_op() {
        let (mut station, train_sender, rx) = create_test_station(10);
        let (_, pool) = station.operate_test(HashMap::new());

        for i in 0..500usize {
            train_sender
                .send(Train::new_values(
                    dict_values(vec![Value::int(i as i64)]),
                    i,
                    0,
                ))
                .unwrap();
        }
        let _ = station.operate(HashMap::new(), pool.clone());
        for i in 0..500usize {
            train_sender
                .send(Train::new_values(
                    dict_values(vec![Value::int((500 + i) as i64)]),
                    500 + i,
                    0,
                ))
                .unwrap();
        }

        let receiver = pool.control_receiver();
        // the station should open a platform, the station starts another platform,  the threshold should be reached and after some time be balanced
        for state in vec![Ready(0), Ready(0)] {
            assert_eq!(state, receiver.recv().unwrap())
        }
        let mut trains = vec![];

        while trains.iter().map(|v: &Train| v.len()).sum::<usize>() < 1_000 {
            trains.push(rx.recv().unwrap())
        }
        //debug!("{:?}", trains);
        let mut ids = vec![];
        for train in &trains {
            ids.push(train.id);
        }

        assert_eq!(
            trains.iter().map(|t| t.len()).sum::<usize>(),
            1_000,
            "{:?}",
            trains.last().unwrap()
        )
    }

    fn run_minimal_overhead(name: &str, values: Vec<Value>) {
        let (mut station, train_sender, rx) = create_test_station(0);
        let (_, pool) = station.operate_test(HashMap::new());

        let mut trains = vec![];
        let amount = 10_000;

        for i in 0..amount {
            trains.push(Train::new_values(values.clone(), i, 0));
        }

        let receiver = pool.control_receiver();
        // the station should open a platform
        for state in vec![Ready(0)] {
            assert_eq!(state, receiver.recv().unwrap())
        }

        let mut values = vec![];
        let time = Instant::now();

        for train in trains {
            train_sender.send(train).unwrap();
        }

        while values.iter().map(|t: &Train| t.len()).sum::<usize>() < amount {
            values.push(rx.recv().unwrap());
        }

        let elapsed = time.elapsed();
        println!(
            "time {}: {:?}, per entry {:?}",
            name,
            elapsed,
            elapsed.div_f64(amount as f64)
        );

        pool.control_sender().send(Stop(0)).unwrap();
    }

    mod overhead {
        use super::*;

        #[test]
        fn minimal_overhead_int() {
            run_minimal_overhead("Int", dict_values(vec![Value::int(3)]));
        }

        #[test]
        fn minimal_overhead_float() {
            run_minimal_overhead("Float", dict_values(vec![Value::float(1.2)]));
        }

        #[test]
        fn minimal_overhead_text() {
            run_minimal_overhead("Text", dict_values(vec![Value::text("test")]));
        }

        #[test]
        fn minimal_overhead_bool() {
            run_minimal_overhead("Bool", dict_values(vec![Value::bool(true)]));
        }

        #[test]
        fn minimal_overhead_array() {
            run_minimal_overhead(
                "Array",
                dict_values(vec![Value::array([1.2.into(), 1.2.into()].into())]),
            );
        }

        #[test]
        fn minimal_overhead_dict() {
            run_minimal_overhead(
                "Dict",
                dict_values(vec![Value::dict_from_kv("age", 25.into())]),
            );
        }
    }

    fn create_test_station(duration: u64) -> (Station, Tx<Train>, Rx<Train>) {
        let mut station = Station::new(0);
        let train_sender = station.get_in();
        let (tx, rx) = new_channel("test", false);
        let _train_receiver = station.add_out(0, tx);
        let time = duration.clone();

        station.set_transform(match duration {
            0 => Transforms::Func(FuncTransform::new(Arc::new(move |_num, value| value))),
            _ => Transforms::Func(FuncTransform::new(Arc::new(move |_num, value| {
                sleep(Duration::from_millis(time));
                value
            }))),
        });

        (station, train_sender, rx)
    }

    #[test]
    #[traced_test]
    fn back_window_per_element_trigger() {
        test_trigger(
            Window::back(20, TimeUnit::Millis),
            TriggerType::Element,
            vec![
                (Value::from(1), 0, 0),
                (Value::from(2), 1, 1),
                (Value::from(3), 2, 2),
            ],
            3,
            false,
        );
    }

    #[test]
    #[traced_test]
    fn interval_window_window_trigger() {
        test_trigger(
            Window::interval(20, TimeUnit::Millis, Time::new(0, 0)),
            TriggerType::WindowEnd,
            vec![
                (Value::from(1), 0, 0),
                (Value::from(2), 1, 1),
                (Value::from(3), 2, 2),
                (Value::from(4), 21, 21), // watermark is last received
            ],
            1,
            false,
        )
    }

    #[test]
    #[traced_test]
    fn back_window_window_trigger_no_overlap() {
        test_trigger(
            Window::back(2, TimeUnit::Millis),
            TriggerType::WindowEnd,
            vec![
                (Value::from(1), 0, 0),
                (Value::from(2), 10, 10),
                (Value::from(3), 20, 20),
            ],
            3,
            false,
        )
    }

    fn test_trigger(
        window: Window,
        trigger: TriggerType,
        values: Vec<(Value, usize, u64)>,
        answers: usize,
        has_more: bool,
    ) {
        let mut station = Station::new(0);
        station.window = window;
        station.trigger = trigger;

        let (tx, rx) = new_channel("Test", false);
        station.add_out(0, tx).unwrap();

        let trains = values
            .into_iter()
            .enumerate()
            .map(|(i, (val, mark, out))| {
                let mut train = Train::new_values(vec![val], i, 0);
                train.event_time = Time::new(mark as i64, 0);
                (train, out)
            })
            .collect::<Vec<(_, _)>>();

        let (_, pool) = station.operate_test(Default::default());
        pool.control_receiver().recv().unwrap(); // wait for go from station
        pool.control_sender().send(Ready(0)).unwrap(); // start station

        let mut time = 0;
        for (train, out) in trains {
            let wait = out - time;
            sleep(Duration::from_millis(wait)); // wait util we can send train
            station.fake_receive(train).unwrap();
            time += wait; // we are farther along
        }

        let result = receive(rx, Duration::from_secs(1));

        if !has_more {
            assert_eq!(result.len(), answers);
        } else {
            assert!(result.len() >= answers);
        }

        pool.control_sender().send(Stop(0)).unwrap(); // stop station
    }

    fn receive(rx: Rx<Train>, duration: Duration) -> Vec<Train> {
        let mut results = vec![];

        let instant = Instant::now();

        while instant.elapsed() < duration {
            if let Ok(train) = rx.try_recv() {
                results.push(train);
            } else {
                sleep(Duration::from_millis(2));
            }
        }
        results
    }
}
