use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::thread;

use crate::processing::layout::Layout;
use crate::processing::plan::PlanStage;
use crate::processing::platform::Platform;
use crate::processing::sender::Sender;
use crate::processing::train::Train;
use crate::processing::transform::Transform;
use crate::processing::window::Window;
use crate::util::{new_channel, new_id, Rx, Tx};
use crossbeam::channel;
use crossbeam::channel::{unbounded, Receiver};
use tracing::{debug, error};

#[derive(Clone)]
pub struct Station {
    pub id: usize,
    pub stop: usize,
    pub incoming: (Tx<Train>, Rx<Train>),
    pub outgoing: Sender,
    pub window: Window,
    pub transform: Option<Transform>,
    pub block: Vec<usize>,
    pub inputs: Vec<usize>,
    pub layout: Layout,
    pub control: (channel::Sender<Command>, Receiver<Command>),
}

impl Default for Station {
    fn default() -> Self {
        Self::new(usize::MAX)
    }
}

impl Station {
    pub(crate) fn new(stop: usize) -> Self {
        let incoming = new_channel();
        let control = unbounded();
        Station {
            id: new_id(),
            stop,
            incoming: (incoming.0, incoming.1),
            outgoing: Sender::default(),
            window: Window::default(),
            transform: None,
            block: vec![],
            inputs: vec![],
            layout: Layout::default(),
            control: (control.0.clone(), control.1.clone()),
        }
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
                PlanStage::Transform => station.set_transform(Transform::parse(&stage.1)?),
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
                            .map_err(|err| format!("Could not parse stop number: {}", err))?,
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

    pub fn derive_output_layout(&self, inputs: HashMap<String, &Layout>) -> Layout {
        if let Some(transform) = self.transform.clone() {
            transform.derive_output_layout(inputs).unwrap_or_default()
        } else {
            inputs.values().next().cloned().cloned().unwrap_or_default()
        }
    }

    pub fn derive_input_layout(&self) -> Layout {
        self.clone()
            .transform
            .map(|t| t.derive_input_layout().unwrap_or_default())
            .unwrap_or_default()
    }

    pub(crate) fn close(&mut self) {
        self.control
            .0
            .send(Command::Stop(0))
            .expect("TODO: panic message");
    }

    pub(crate) fn set_stop(&mut self, stop: usize) {
        self.stop = stop
    }

    pub(crate) fn set_window(&mut self, window: Window) {
        self.window = window;
    }

    pub(crate) fn set_transform(&mut self, transform: Transform) {
        self.transform = Some(transform);
    }

    pub(crate) fn add_block(&mut self, line: usize) {
        self.block.push(line);
    }

    pub(crate) fn add_out(&mut self, id: usize, out: Tx<Train>) -> Result<(), String> {
        self.outgoing.add(id, out);
        Ok(())
    }

    pub(crate) fn send(&mut self, train: Train) -> Result<(), String> {
        self.incoming.0.send(train).map_err(|e| e.to_string())
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

    pub(crate) fn operate(
        &mut self,
        control: Arc<channel::Sender<Command>>,
        transforms: HashMap<String, Transform>,
    ) -> channel::Sender<Command> {
        let (mut platform, sender) = Platform::new(self, transforms);
        let stop = self.stop;

        match thread::Builder::new()
            .name(format!("Station {}", self.id))
            .spawn(move || {
                debug!("Starting station {}", stop);
                platform.operate(control)
            }) {
            Ok(_) => {}
            Err(err) => error!("Could not spawn thread: {}", err),
        };
        sender
    }

    fn add_explicit_layout(&mut self, layout: Layout) {
        self.layout = layout;
    }
}

#[derive(Clone)]
pub enum Command {
    Stop(usize),
    Ready(usize),
    Overflow(usize),
    Threshold(usize),
    Okay(usize),
    Attach(usize, Tx<Train>),
    Detach(usize),
}

#[cfg(test)]
impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (_, _) => todo!(),
        }
    }
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Stop(s) => f.debug_tuple("Stop").field(s).finish(),
            Command::Ready(r) => f.debug_tuple("Ready").field(r).finish(),
            Command::Overflow(o) => f.debug_tuple("Overflow").field(o).finish(),
            Command::Threshold(t) => f.debug_tuple("Threshold").field(t).finish(),
            Command::Okay(o) => f.debug_tuple("Okay").field(o).finish(),
            Command::Attach(id, _) => f.debug_tuple("Attach").field(id).finish(),
            Command::Detach(id) => f.debug_tuple("Detach").field(id).finish(),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use crate::processing::plan::Plan;
    use crate::processing::station::Command::{Okay, Ready, Threshold};
    use crate::processing::station::{Command, Station};
    pub use crate::processing::tests::dict_values;
    use crate::processing::train::Train;
    use crate::processing::transform::{FuncTransform, Transform};
    use crate::util::{new_channel, Rx, Tx};
    use crate::value::{Dict, Value};
    use crossbeam::channel::{unbounded, Receiver, Sender};

    #[test]
    fn start_stop_test() {
        let mut station = Station::new(0);

        let control = unbounded();

        let mut values = dict_values(vec![
            Value::text("test"),
            Value::bool(true),
            Value::float(3.3),
            Value::null(),
        ]);

        for x in 0..1_000_000 {
            values.push(Value::Dict(Dict::from(Value::int(x))))
        }

        let (tx, rx) = new_channel();

        station.add_out(0, tx).unwrap();
        station.operate(Arc::new(control.0), HashMap::new());
        station.send(Train::new(values.clone())).unwrap();

        let res = rx.recv();
        match res {
            Ok(mut t) => {
                assert_eq!(
                    values.len(),
                    t.values.clone().map_or(usize::MAX, |values| values.len())
                );
                for (i, value) in t.values.take().unwrap().iter().enumerate() {
                    assert_eq!(value, &values[i]);
                    assert_ne!(&Value::text(""), value.as_dict().unwrap().get("$").unwrap())
                }
            }
            Err(..) => assert!(false),
        }
    }

    #[test]
    fn station_two_train() {
        let values = vec![Value::Dict(Dict::from(Value::int(3)))];
        let (tx, _rx) = unbounded();
        let control = Arc::new(tx);

        let mut first = Station::new(0);
        let input = first.get_in();

        let (output_tx, output_rx) = new_channel();

        let mut second = Station::new(1);
        second.add_out(0, output_tx).unwrap();

        let tx = second.get_in();
        first.add_out(1, tx).unwrap();

        first.operate(Arc::clone(&control), HashMap::new());
        second.operate(Arc::clone(&control), HashMap::new());

        input.send(Train::new(values.clone())).unwrap();

        let res = output_rx.recv().unwrap();
        assert_eq!(res.values.clone().unwrap(), values);
        assert_ne!(res.values.clone().unwrap(), vec![Value::null().into()]);

        assert!(output_rx.try_recv().is_err());

        drop(input); // close the channel
        first.close();
        second.close();
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
        let (mut station, train_sender, _rx, c_rx, a_tx) = create_test_station(10);

        let sender = station.operate(Arc::clone(&a_tx), HashMap::new());
        sender.send(Threshold(3)).unwrap();

        for _ in 0..1_000 {
            train_sender
                .send(Train::new(dict_values(vec![Value::int(3)])))
                .unwrap();
        }

        // the station should start, the threshold should be reached and after some time be balanced
        for state in vec![Ready(0), Threshold(0), Okay(0)] {
            assert_eq!(state, c_rx.recv().unwrap())
        }
    }

    fn too_high_two() {
        let (mut station, train_sender, _rx, c_rx, a_tx) = create_test_station(100);

        let sender = station.operate(Arc::clone(&a_tx), HashMap::new());
        sender.send(Threshold(3)).unwrap();
        // second platform
        station.operate(Arc::clone(&a_tx), HashMap::new());

        for _ in 0..1_000 {
            train_sender
                .send(Train::new(dict_values(vec![Value::int(3)])))
                .unwrap();
        }

        // the station should open a platform, the station starts another platform,  the threshold should be reached and after some time be balanced
        for state in vec![Ready(0), Ready(0), Threshold(0), Okay(0)] {
            assert_eq!(state, c_rx.recv().unwrap())
        }
    }

    #[test]
    fn remove_during_op() {
        let (mut station, train_sender, rx, c_rx, a_tx) = create_test_station(10);
        let _sender = station.operate(Arc::clone(&a_tx), HashMap::new());

        for _ in 0..500 {
            train_sender
                .send(Train::new(dict_values(vec![Value::int(3)])))
                .unwrap();
        }
        station.operate(Arc::clone(&a_tx), HashMap::new());
        for _ in 0..500 {
            train_sender
                .send(Train::new(dict_values(vec![Value::int(3)])))
                .unwrap();
        }

        // the station should open a platform, the station starts another platform,  the threshold should be reached and after some time be balanced
        for state in vec![Ready(0), Ready(0)] {
            assert_eq!(state, c_rx.recv().unwrap())
        }
        let mut values = vec![];

        while values.len() < 1_000 {
            values.push(rx.recv().unwrap())
        }
        assert_eq!(values.len(), 1_000)
    }

    // fix for parameterized
    mod overhead_tests {
        use crate::processing::station::tests::create_test_station;
        use crate::processing::station::tests::Value;
        use crate::processing::station::Command::Ready;
        pub use crate::processing::tests::dict_values;
        use crate::processing::Train;
        use rstest::rstest;
        use std::collections::HashMap;
        use std::sync::Arc;
        use std::time::Instant;

        #[rstest]
        #[case("Int", dict_values(vec![Value::int(3)]))]
        #[case("Float", dict_values(vec![Value::float(1.2)]))]
        #[case("Text", dict_values(vec![Value::text("test")]))]
        #[case("Bool", dict_values(vec![Value::bool(true)]))]
        #[case("Array", dict_values(vec![Value::array([1.2.into(), 1.2.into()].into())]))]
        #[case("Dict", dict_values(vec![Value::dict_from_kv("age", 25.into())]))]
        pub fn minimal_overhead(#[case] name: &str, #[case] values: Vec<Value>) {
            let (mut station, train_sender, rx, c_rx, a_tx) = create_test_station(0);

            let _sender = station.operate(Arc::clone(&a_tx), HashMap::new());

            let mut trains = vec![];

            let amount = 1_000;

            for _ in 0..amount {
                trains.push(Train::new(values.clone()));
            }

            // the station should open a platform
            for state in vec![Ready(0)] {
                assert_eq!(state, c_rx.recv().unwrap())
            }
            let time = Instant::now();

            for train in trains {
                train_sender.send(train).unwrap();
            }

            for _ in 0..amount {
                let _ = rx.recv();
            }

            let elapsed = time.elapsed().as_nanos();
            println!(
                "time {}: {} nanos, per entry {}ns",
                name,
                elapsed,
                elapsed / amount
            );
        }
    }

    fn create_test_station(
        duration: u64,
    ) -> (
        Station,
        Tx<Train>,
        Rx<Train>,
        Receiver<Command>,
        Arc<Sender<Command>>,
    ) {
        let mut station = Station::new(0);
        let train_sender = station.get_in();
        let (tx, rx) = new_channel();
        let _train_receiver = station.add_out(0, tx);
        let time = duration.clone();

        station.set_transform(match duration {
            0 => Transform::Func(FuncTransform::new(Arc::new(move |_num, value| value))),
            _ => Transform::Func(FuncTransform::new(Arc::new(move |_num, value| {
                sleep(Duration::from_millis(time));
                value
            }))),
        });

        let (c_tx, c_rx) = unbounded();

        let a_tx = Arc::new(c_tx);
        (station, train_sender, rx, c_rx, a_tx)
    }
}
