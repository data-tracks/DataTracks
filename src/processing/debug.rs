use crate::processing::destination::Destination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::util::{new_channel, new_id, Rx, Tx};
use crossbeam::channel::{unbounded, Sender};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::thread;
use tracing::{debug, error};

pub struct DebugDestination {
    id: usize,
    receiver: Option<Rx<Train>>,
    sender: Tx<Train>,
}

impl DebugDestination {
    pub fn new() -> Self {
        let (tx, _num, rx) = new_channel();
        DebugDestination { id: new_id(), receiver: Some(rx), sender: tx }
    }
}

impl Configurable for DebugDestination {
    fn options(&self) -> Map<String, Value> {
        Map::new()
    }

    fn name(&self) -> String {
        String::from("Debug")
    }
}

impl Destination for DebugDestination {
    fn parse(_options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        Ok(DebugDestination::new())
    }

    fn operate(&mut self, _control: Arc<Sender<Command>>) -> Sender<Command> {
        let receiver = self.receiver.take().unwrap();
        let (tx, _rx) = unbounded();

        thread::spawn(move || {
            let mut writer = None;
            if let Ok(file) = File::create("debug.txt") {
                writer = Some(BufWriter::new(file));
            }
            loop {
                let res = receiver.recv();
                match res {
                    Ok(train) => {
                        if let Some(ref mut w) = writer {
                            writeln!(w, "{:?}", train).expect("Could not write to debug file.");
                        }

                        debug!("last: {}, {:?}", train.last(), train.values.unwrap_or(vec![]));
                    }
                    Err(e) => {
                        error!("{}", e)
                    }
                }

                if let Some(ref mut w) = writer {
                    w.flush().unwrap();
                }

            }
        });
        tx
    }

    fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }


    fn get_id(&self) -> usize {
        self.id
    }


    fn serialize(&self) -> DestinationModel {
        DestinationModel { type_name: String::from("Debug"), id: self.id.to_string(), configs: HashMap::new() }
    }

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized,
    {
        Some(DestinationModel { type_name: String::from("Debug"), id: String::from("Debug"), configs: HashMap::new() })
    }
}