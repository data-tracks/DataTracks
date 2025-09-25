use crate::processing::Train;
use crate::processing::destination::Destination;
use crate::util::HybridThreadPool;
use crate::util::{Rx, Tx};
use core::ConfigModel;
use core::Configurable;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use tracing::{debug, error};
use error::error::TrackError;

pub struct DebugDestination {
    receiver: Option<Rx<Train>>,
}

impl Default for DebugDestination {
    fn default() -> Self {
        Self::new()
    }
}

impl DebugDestination {
    pub fn new() -> Self {
        DebugDestination { receiver: None }
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

impl TryFrom<HashMap<String, ConfigModel>> for DebugDestination {
    type Error = String;

    fn try_from(value: HashMap<String, ConfigModel>) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl Destination for DebugDestination {
    fn parse(_options: Map<String, Value>) -> Result<Self, TrackError>
    where
        Self: Sized,
    {
        Ok(DebugDestination::new())
    }

    fn operate(&mut self, id: usize, tx: Tx<Train>, pool: HybridThreadPool) -> Result<usize, TrackError> {
        let receiver = self.receiver.take().unwrap();

        pool.execute_sync("Debug Destination", move |_args| {
            let mut writer = None;
            if let Ok(file) = File::create("../../../debug.txt") {
                writer = Some(BufWriter::new(file));
            }
            loop {
                match receiver.recv() {
                    Ok(train) => {
                        if let Some(ref mut w) = writer {
                            writeln!(w, "{train:?}").expect("Could not write to debug file.");
                        }

                        debug!("last: {}, {:?}", train.last(), train.content);
                    }
                    Err(e) => {
                        error!("{}", e)
                    }
                }

                if let Some(ref mut w) = writer {
                    w.flush().unwrap();
                }
            }
        })
    }

    fn type_(&self) -> String {
        String::from("Debug")
    }

    fn get_configs(&self) -> HashMap<String, ConfigModel> {
        HashMap::new()
    }
}
