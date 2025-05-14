use std::collections::HashMap;
use std::sync::Arc;

use crate::mqtt::MqttSource;
use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::station::Command;
#[cfg(test)]
use crate::processing::tests::DummySource;
use crate::processing::train::Train;
use crate::sql::LiteSource;
use crate::ui::ConfigModel;
use crate::util::Tx;
use crossbeam::channel::Sender;
use serde_json::{Map, Value};
use crate::processing::HttpSource;

pub fn parse_source(type_: &str, options: Map<String, Value>) -> Result<Box<dyn Source>, String> {
    let source: Box<dyn Source> = match type_.to_ascii_lowercase().as_str() {
        "mqtt" => Box::new(MqttSource::parse(options)?),
        "sqlite" => Box::new(LiteSource::parse(options)?),
        "http" => Box::new(HttpSource::parse(options)?),
        #[cfg(test)]
        "dummy" => Box::new(DummySource::parse(options)?),
        _ => Err(format!("Invalid type: {}", type_))?,
    };
    Ok(source)
}

pub trait Source: Send + Sync + Configurable {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized;

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;

    fn add_out(&mut self, out: Tx<Train>) {
        self.outs().push(out);
    }

    fn outs(&mut self) -> &mut Vec<Tx<Train>>;

    fn id(&self) -> usize;

    fn dump_source(&self, include_id: bool) -> String {
        Configurable::dump(self).to_string()
    }

    fn serialize(&self) -> SourceModel;

    fn from(configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
    where
        Self: Sized;

    fn serialize_default() -> Result<SourceModel, ()>
    where
        Self: Sized;

}


