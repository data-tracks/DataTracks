use std::sync::Arc;

use crate::http::destination::HttpDestination;
use crate::mqtt::MqttDestination;
use crate::processing::option::Configurable;
use crate::processing::plan::DestinationModel;
use crate::processing::station::Command;
#[cfg(test)]
use crate::processing::tests::DummyDestination;
use crate::sql::LiteDestination;
use crate::tpc::TpcDestination;
use crate::util::Tx;
use crossbeam::channel::Sender;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use track_rails::message_generated::protocol::{Destination as FlatDestination, DestinationArgs};
use serde_json::{Map, Value};
#[cfg(test)]
use std::sync::Mutex;
use value::train::Train;

pub fn parse_destination(
    type_: &str,
    options: Map<String, Value>,
) -> Result<Box<dyn Destination>, String> {
    let destination: Box<dyn Destination> = match type_.to_ascii_lowercase().as_str() {
        "mqtt" => Box::new(MqttDestination::parse(options)?),
        "sqlite" => Box::new(LiteDestination::parse(options)?),
        "http" => Box::new(HttpDestination::parse(options)?),
        "tpc" => Box::new(TpcDestination::parse(options)?),
        #[cfg(test)]
        "dummy" => Box::new(DummyDestination::parse(options)?),
        _ => Err(format!("Invalid type: {}", type_))?,
    };
    Ok(destination)
}

pub trait Destination: Send + Configurable + Sync {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized;

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command>;
    fn get_in(&self) -> Tx<Train>;

    fn id(&self) -> usize;

    fn type_(&self) -> String;

    fn dump_destination(&self, _include_id: bool) -> String {
        Configurable::dump(self)
    }

    fn serialize(&self) -> DestinationModel;

    fn flatternize<'a>(
        &self,
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<FlatDestination<'a>> {
        let name = Some(builder.create_string(&self.name().to_string()));
        let type_ = Some(builder.create_string(&self.type_().to_string()));

        FlatDestination::create(
            builder,
            &DestinationArgs {
                id: self.id() as u64,
                name,
                type_,
            },
        )
    }

    fn serialize_default() -> Option<DestinationModel>
    where
        Self: Sized;

    #[cfg(test)]
    fn get_result_handle(&self) -> Arc<Mutex<Vec<Train>>> {
        panic!()
    }
}
