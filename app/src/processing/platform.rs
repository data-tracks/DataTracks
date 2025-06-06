use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::{sleep, Builder};
use std::time::Duration;

use crate::algebra::{Executor, IdentityIterator};
use crate::optimize::OptimizeStrategy;
use crate::processing::layout::Layout;
use crate::processing::select::{TriggerSelector, WindowSelector};
use crate::processing::sender::Sender;
use crate::processing::station::Command::{Attach, Detach, Okay, Ready, Threshold};
use crate::processing::station::{Command, Station};
use crate::processing::transform::Transform;
use crate::processing::watermark::WatermarkStrategy;
use crate::processing::window::Window;
use crate::processing::Train;
use crate::util::new_channel;
use crate::util::Tx;
use crate::util::{new_id, Rx};
use crossbeam::channel;
use crossbeam::channel::Receiver;
pub use logos::Source;
use parking_lot::RwLock;
use tracing::{debug, error, info};

const IDLE_TIMEOUT: Duration = Duration::from_nanos(10);

// What: Transformations, Where: Windowing, When: Triggers, How: Accumulation

pub(crate) struct Platform {
    id: usize,
    control: Receiver<Command>,
    receiver: Rx<Train>,
    sender: Sender,
    transform: Option<Transform>,
    layout: Layout,
    window: Window,
    watermark_strategy: WatermarkStrategy,
    stop: usize,
    blocks: Vec<usize>,
    inputs: Vec<usize>,
    transforms: HashMap<String, Transform>,
}

impl Platform {
    pub(crate) fn new(
        station: &mut Station,
        transforms: HashMap<String, Transform>,
    ) -> (Self, channel::Sender<Command>) {
        let receiver = station.incoming.1.clone();
        let sender = station.outgoing.clone();
        let transform = station.transform.clone();
        let window = station.window.clone();
        let stop = station.stop;
        let blocks = station.block.clone();
        let inputs = station.inputs.clone();
        let control = station.control.clone();
        let layout = station.layout.clone();
        let watermark_strategy = station.watermark_strategy.clone();

        (
            Platform {
                id: new_id(),
                control: control.1,
                receiver,
                sender,
                transform,
                window,
                layout,
                stop,
                blocks,
                inputs,
                transforms,
                watermark_strategy,
            },
            control.0,
        )
    }
    pub(crate) fn operate(&mut self, control: Arc<channel::Sender<Command>>) {
        let process = optimize(
            self.stop,
            self.transform.clone(),
            self.transforms.clone(),
            self.sender.clone(),
        );

        let stop = self.stop;
        let timeout = IDLE_TIMEOUT;
        let mut threshold = 2000;
        let mut too_high = false;

        let mut watermark_strategy = Arc::new(Mutex::new(self.watermark_strategy.clone()));

        let storage = Arc::new(Mutex::new(vec![]));

        let window_selector = Arc::new(RwLock::new(WindowSelector::new(self.window.clone())));

        let trigger_selector = TriggerSelector::new(storage.clone().into());

        let when_tx = when(
            watermark_strategy.clone(),
            window_selector.clone(),
            trigger_selector,
            process,
        );

        control.send(Ready(stop)).unwrap();

        loop {
            // are we struggling to handle incoming?
            let current = self.receiver.len();
            if current > threshold && !too_high {
                control.send(Threshold(stop)).unwrap();
                too_high = true;
            } else if current < threshold && too_high {
                control.send(Okay(stop)).unwrap();
                too_high = false;
            }

            // did we get a command?
            if let Ok(command) = self.control.try_recv() {
                match command {
                    Command::Stop(_) => {
                        match when_tx.send(Command::Stop(stop)) {
                            Ok(_) => {}
                            Err(err) => {
                                error!("cannot stop trigger {err}")
                            }
                        }
                        return;
                    }
                    Attach(num, (send, watermark)) => {
                        watermark_strategy
                            .lock()
                            .unwrap()
                            .attach(num, watermark.clone());
                        when_tx.send(Attach(num, (send, watermark))).unwrap();
                    }
                    Detach(num) => {
                        when_tx.send(Detach(num)).unwrap();
                        watermark_strategy.lock().unwrap().detach(num);
                    }
                    Threshold(th) => {
                        threshold = th;
                    }
                    _ => {}
                }
            }

            match self.receiver.try_recv() {
                Ok(train) => {
                    debug!("{:?}", train);
                    if self.layout.fits_train(&train) {
                        // save and update if something changed
                        storage.lock().unwrap().push(train.clone());
                        watermark_strategy.lock().unwrap().mark(&train);
                        window_selector.write().mark(&train);
                    }
                }
                _ => {
                    sleep(timeout); // wait again
                }
            };
        }
    }
}

fn when(
    watermark_strategy: Arc<Mutex<WatermarkStrategy>>,
    where_: Arc<RwLock<WindowSelector>>,
    mut when: TriggerSelector,
    mut what: Executor,
) -> Tx<Command> {
    let (tx, rx) = new_channel::<Command, &str>("Trigger");
    // shall we?
    // - specific window?
    // - specific watermark
    // - always
    // take what we need -> window
    // - which window?
    // apply transformation
    // send out
    let result = Builder::new()
        .name(String::from("when"))
        .spawn(move || loop {
            match rx.try_recv() {
                Ok(cmd) => match cmd {
                    Command::Stop(_) => return,
                    Attach(num, (observe, _)) => {
                        what.attach(num, observe.clone());
                    }
                    Detach(num) => {
                        what.detach(num);
                    }
                    _ => {}
                },
                Err(_) => {}
            }

            // get all "changed" windows
            let windows = where_.write().select();

            let current = watermark_strategy.lock().unwrap().current();

            // decide if we fire a window, discard or wait
            match when.select(windows, &current) {
                trains if !trains.is_empty() => {
                    let train = trains
                        .into_iter()
                        .map(|(_, t)| t)
                        .reduce(|a, b| a.merge(b))
                        .unwrap();
                    what.execute(train);
                }
                _ => {}
            }
        });
    match result {
        Ok(_) => {}
        Err(err) => error!("{}", err),
    }

    tx
}

fn optimize(
    stop: usize,
    transform: Option<Transform>,
    transforms: HashMap<String, Transform>,
    sender: Sender,
) -> Executor {
    let enumerator = match transform {
        Some(transform) => transform.optimize(transforms, Some(OptimizeStrategy::rule_based())),
        None => Box::new(IdentityIterator::new()),
    };
    Executor::new(stop, enumerator, sender)
}
