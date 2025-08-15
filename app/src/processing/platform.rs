use std::collections::HashMap;
use std::sync::Arc;
use std::thread::{sleep, Builder};
use std::time::Duration;

use crate::algebra::Executor;
use crate::optimize::OptimizeStrategy;
use crate::processing::layout::Layout;
use crate::processing::portal::Portal;
use crate::processing::select::{TriggerSelector, WindowSelector};
use crate::processing::sender::Sender;
use crate::processing::station::Station;
use crate::processing::transform::Transforms;
use crate::processing::watermark::WatermarkStrategy;
use crate::processing::window::Window;
use crate::processing::Train;
use crate::util::TriggerType;
use crate::util::{new_channel, Rx, Tx};
use crate::util::{new_id, WorkerMeta};
pub use logos::Source;
use parking_lot::RwLock;
use threading::command::Command;
use threading::command::Command::{Attach, Detach, Okay, Ready, Threshold};
use tracing::{error};

const IDLE_TIMEOUT: Duration = Duration::from_nanos(10);
const BATCH_SIZE: usize = 100;

// What: Transformations, Where: Windowing, When: Triggers, How: Accumulation
/// Platform represents an independent action steps which handles data based on the 4 streaming operations from different inputs  
pub(crate) struct Platform {
    id: usize,
    receiver: Rx<Train>,
    sender: Sender,
    transform: Option<Transforms>,
    layout: Layout,
    window: Window,
    trigger: TriggerType,
    watermark_strategy: WatermarkStrategy,
    stop: usize,
    blocks: Vec<usize>,
    inputs: Vec<usize>,
    transforms: HashMap<String, Transforms>,
}

impl Platform {
    pub(crate) fn new(station: &mut Station, transforms: HashMap<String, Transforms>) -> Self {
        let receiver = station.incoming.1.clone();
        let sender = station.outgoing.clone();
        let transform = station.transform.clone();
        let window = station.window.clone();
        let stop = station.stop;
        let blocks = station.block.clone();
        let inputs = station.inputs.clone();
        let layout = station.layout.clone();
        let trigger = station.trigger.clone();
        let watermark_strategy = station.watermark_strategy.clone();

        Platform {
            id: new_id(),
            receiver,
            sender,
            transform,
            window,
            layout,
            stop,
            trigger,
            blocks,
            inputs,
            transforms,
            watermark_strategy,
        }
    }

    pub(crate) fn operate(&mut self, meta: WorkerMeta) -> Result<(), String> {
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

        let watermark_strategy = self.watermark_strategy.clone();

        let portal = Portal::new()?;

        let window_selector = Arc::new(RwLock::new(WindowSelector::new(self.window.clone())));

        let trigger_selector = TriggerSelector::new(portal.clone(), self.trigger.clone());

        let when_tx = when(
            self.stop,
            watermark_strategy.clone(),
            window_selector.clone(),
            trigger_selector,
            process,
        );

        meta.output_channel.send(Ready(stop))?;

        loop {
            // are we struggling to handle incoming?
            let current = self.receiver.len();
            if current > threshold && !too_high {
                meta.output_channel.send(Threshold(stop))?;
                too_high = true;
            } else if current < threshold && too_high {
                meta.output_channel.send(Okay(stop))?;
                too_high = false;
            }

            // did we get a command?
            if let Ok(command) = meta.ins.1.try_recv() {
                match command {
                    Command::Stop(_) => {
                        when_tx.send(Command::Stop(stop))?;
                        return Ok(());
                    }
                    Attach(num, (send, watermark)) => {
                        watermark_strategy.attach(num, watermark.clone());
                        when_tx.send(Attach(num, (send, watermark)))?;
                    }
                    Detach(num) => {
                        when_tx.send(Detach(num))?;
                        watermark_strategy.detach(num);
                    }
                    Threshold(th) => {
                        threshold = th;
                    }
                    _ => {}
                }
            }

            let mut i = 0;
            let mut trains = Vec::new();
            let mut finish = false;
            while i < BATCH_SIZE && !finish {
                match self.receiver.try_recv() {
                    Ok(t) => {
                        trains.push(t);
                    }
                    Err(_) => {
                        finish = true;
                    }
                }

                i += 1;
            }

            if !trains.is_empty() {
                //debug!("{:?} trains in", trains);
                // save and update if something changed
                portal.push_trains(trains.clone());
                for t in trains {
                    watermark_strategy.mark(&t);
                    window_selector.write().mark(&t);
                }
            } else {
                sleep(timeout);
            }
        }
    }
}

fn when(
    stop: usize,
    watermark_strategy: WatermarkStrategy,
    where_: Arc<RwLock<WindowSelector>>,
    mut when: TriggerSelector,
    mut what: Executor,
) -> Tx<Command> {
    let (tx, rx) = new_channel::<Command, &str>("Trigger", false);
    // shall we?
    // - specific window?
    // - specific watermark
    // - always
    // take what we need -> window
    // - which window?
    // apply transformation
    // send out
    let result = Builder::new().name(format!("when {stop}")).spawn(move || {
        loop {
            if let Ok(cmd) = rx.try_recv() {
                match cmd {
                    Command::Stop(_) => return,
                    Attach(num, (observe, _)) => {
                        what.attach(num, observe.clone());
                    }
                    Detach(num) => {
                        what.detach(num);
                    }
                    _ => {}
                }
            }

            let current = watermark_strategy.current();

            // get all "changed" windows
            let windows = where_.write().select(current);
            if windows.is_empty() {
                continue;
            }

            // decide if we fire a window, discard or wait
            match when.select(windows, &current) {
                trains if !trains.is_empty() => {
                    //debug!("trains {:?}", trains);

                    match trains.into_iter().try_for_each(|(_, t)| what.execute(t)) {
                        Ok(_) => {}
                        Err(err) => println!("{:?} train error", err),
                    };
                }
                _ => {}
            }
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
    transform: Option<Transforms>,
    transforms: HashMap<String, Transforms>,
    sender: Sender,
) -> Executor {
    let transforms = transforms.iter().map(|(k, v)| (k.clone(), v.optimize(HashMap::new(), None))).collect(); // this would require change if cycles should be possible

    let enumerator = transform
        .map(|transform| transform.optimize(transforms, Some(OptimizeStrategy::rule_based())));
    Executor::new(stop, enumerator, sender)
}
