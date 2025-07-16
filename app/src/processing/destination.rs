use std::ops::{Deref, DerefMut};
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
use crate::processing::destination::Destinations::{Http, Lite, Mqtt, Tpc};
#[cfg(test)]
use crate::processing::destination::Destinations::Dummy;

pub fn parse_destination(
    type_: &str,
    options: Map<String, Value>,
) -> Result<Destinations, String> {
    let destination = match type_.to_ascii_lowercase().as_str() {
        "mqtt" => Mqtt(MqttDestination::parse(options)?),
        "sqlite" => Lite(LiteDestination::parse(options)?),
        "http" => Http(HttpDestination::parse(options)?),
        "tpc" => Tpc(TpcDestination::parse(options)?),
        #[cfg(test)]
        "dummy" => Dummy(DummyDestination::parse(options)?),
        _ => Err(format!("Invalid type: {}", type_))?,
    };
    Ok(destination)
}

#[derive(Clone)]
pub enum Destinations{
    Mqtt(MqttDestination),
    Lite(LiteDestination),
    Http(HttpDestination),
    Tpc(TpcDestination),
    #[cfg(test)]
    Dummy(DummyDestination),
}

impl Deref for Destinations{
    type Target = dyn Destination;

    fn deref(&self) -> &Self::Target {
        match self {
            Mqtt(m) => m,
            Lite(l) => l,
            Http(h) => h,
            Tpc(t) => t,
            #[cfg(test)]
            Dummy(d) => d
        }
    }
}

impl DerefMut for Destinations{
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Mqtt(m) => m,
            Lite(l) => l,
            Http(h) => h,
            Tpc(t) => t,
            #[cfg(test)]
            Dummy(d) => d
        }
    }
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
