use crate::processing::Train;
use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::util::{HybridThreadPool, new_channel, new_id};
use crate::util::{Rx, Tx};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use tracing::{debug, error};

pub struct DebugDestination {
    id: usize,
    receiver: Option<Rx<Train>>,
    sender: Tx<Train>,
}

impl DebugDestination {
    pub fn new() -> Self {
        let (tx, rx) = new_channel("Debug Destination", false);
        DebugDestination {
            id: new_id(),
            receiver: Some(rx),
            sender: tx,
        }
    }
}

impl Configurable for DebugDestination {
    fn name(&self) -> String {
        String::from("Debug")
    }

    fn options(&self) -> Map<String, Value> {
        Map::new()
    }
}

impl Destination for DebugDestination {
    fn parse(_options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        Ok(DebugDestination::new())
    }

    fn operate(&mut self, pool: HybridThreadPool) -> usize {
        let receiver = self.receiver.take().unwrap();

        pool.execute_sync(
            "Debug Destination",
            move |_args| {
                let mut writer = None;
                if let Ok(file) = File::create("../../../debug.txt") {
                    writer = Some(BufWriter::new(file));
                }
                loop {
                    match receiver.recv() {
                        Ok(train) => {
                            if let Some(ref mut w) = writer {
                                writeln!(w, "{:?}", train).expect("Could not write to debug file.");
                            }

                            debug!("last: {}, {:?}", train.last(), train.values);
                        }
                        Err(e) => {
                            error!("{}", e)
                        }
                    }

                    if let Some(ref mut w) = writer {
                        w.flush().unwrap();
                    }
                }
            },
            vec![],
        )
    }

    fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }

    fn id(&self) -> usize {
        self.id
    }

    fn type_(&self) -> String {
        String::from("Debug")
    }

    fn serialize(&self) -> DestinationModel {
        DestinationModel {
            type_name: String::from("Debug"),
            id: self.id.to_string(),
            configs: HashMap::new(),
        }
    }

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized,
    {
        Some(DestinationModel {
            type_name: String::from("Debug"),
            id: String::from("Debug"),
            configs: HashMap::new(),
        })
    }
}
