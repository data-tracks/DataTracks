use crate::http::destination::HttpDestination;
use crate::mongo::MongoDbDestination;
use crate::mqtt::MqttDestination;
use crate::postgres::PostgresDestination;
#[cfg(test)]
use crate::processing::destination::Destinations::Dummy;
use crate::processing::destination::Destinations::{Http, Lite, Mongo, Mqtt, Postgres, Tpc};
use crate::sqlite::LiteDestination;
#[cfg(test)]
use crate::tests::DummyDestination;
use crate::tpc::TpcDestination;
use crate::util::{HybridThreadPool, Tx, new_id};
use core::ConfigModel;
use core::Configurable;
use core::DestinationModel;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
#[cfg(test)]
use std::sync::{Arc, Mutex};
use threading::channel::new_broadcast;
use track_rails::message_generated::protocol::{Destination as FlatDestination, DestinationArgs};
use error::error::TrackError;
use value::train::Train;

#[derive(Clone)]
pub struct DestinationHolder {
    id: usize,
    pub sender: Tx<Train>,
    destination: Destinations,
}

impl DestinationHolder {
    pub fn new(destination: Destinations) -> Self {
        destination.into()
    }

    pub(crate) fn get_in(&self) -> Tx<Train> {
        self.sender.clone()
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn serialize(&self) -> DestinationModel {
        let configs = self.get_configs();
        DestinationModel {
            type_name: self.type_().to_string(),
            id: self.id.to_string(),
            configs,
        }
    }

    pub(crate) fn flatternize<'a>(
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
}

impl Deref for DestinationHolder {
    type Target = Destinations;
    fn deref(&self) -> &Destinations {
        &self.destination
    }
}

impl DerefMut for DestinationHolder {
    fn deref_mut(&mut self) -> &mut Destinations {
        &mut self.destination
    }
}

impl Into<DestinationHolder> for Destinations {
    fn into(self) -> DestinationHolder {
        #[cfg(test)]
        {
            if let Dummy(d) = self {
                return DestinationHolder {
                    id: d.id,
                    sender: new_broadcast(format!("{} Destination", d.name())),
                    destination: Dummy(d),
                };
            }
        }

        DestinationHolder {
            id: new_id(),
            sender: new_broadcast(format!("{} Destination", self.name())),
            destination: self,
        }
    }
}

impl TryFrom<(String, Map<String, Value>)> for Destinations {
    type Error = TrackError;

    fn try_from(value: (String, Map<String, Value>)) -> Result<Self, Self::Error> {
        let destination_type = value.0;
        let options = value.1;
        let destination = match destination_type.to_lowercase().as_str() {
            "mqtt" => Mqtt(MqttDestination::parse(options)?),
            "sqlite" => Lite(LiteDestination::parse(options)?),
            "http" => Http(HttpDestination::parse(options)?),
            "postgres" | "pg" | "postgresql" => Postgres(PostgresDestination::parse(options)?),
            "mongo" | "mongodb" => Mongo(MongoDbDestination::parse(options)?),
            "tpc" => Tpc(TpcDestination::parse(options)?),
            #[cfg(test)]
            "dummy" => Dummy(DummyDestination::parse(options)?),
            _ => Err(format!("Invalid destination type: {destination_type}"))?,
        };
        Ok(destination)
    }
}

impl TryFrom<(String, HashMap<String, ConfigModel>)> for Destinations {
    type Error = TrackError;

    fn try_from(value: (String, HashMap<String, ConfigModel>)) -> Result<Self, Self::Error> {
        let source_type = value.0;
        let options = value.1;
        let source = match source_type.to_lowercase().as_str() {
            "mqtt" => Mqtt(MqttDestination::try_from(options)?),
            "sqlite" => Lite(LiteDestination::try_from(options)?),
            "http" => Http(HttpDestination::try_from(options)?),
            "tpc" => Tpc(TpcDestination::try_from(options)?),
            "mongo" | "mongodb" => Mongo(MongoDbDestination::try_from(options)?),
            "postgres" | "postgresql" => Postgres(PostgresDestination::try_from(options)?),
            #[cfg(test)]
            "dummy" => panic!("Not supported"),
            _ => Err(format!("Invalid destination type: {source_type}"))?,
        };
        Ok(source)
    }
}

#[derive(Clone)]
pub enum Destinations {
    Mqtt(MqttDestination),
    Lite(LiteDestination),
    Postgres(PostgresDestination),
    Http(HttpDestination),
    Mongo(MongoDbDestination),
    Tpc(TpcDestination),
    #[cfg(test)]
    Dummy(DummyDestination),
}

impl Destinations {
    pub(crate) fn get_default_configs() -> Vec<DestinationModel> {
        let values = vec![
            ("MQTT".to_string(), MqttDestination::get_default_configs()),
            ("HTTP".to_string(), HttpDestination::get_default_configs()),
        ];

        values
            .into_iter()
            .map(|(name, config)| Self::serialize_default(name, config).unwrap())
            .collect()
    }

    fn serialize_default(
        name: String,
        configs: HashMap<String, ConfigModel>,
    ) -> Result<DestinationModel, ()>
    where
        Self: Sized,
    {
        Ok(DestinationModel {
            type_name: name.clone(),
            id: name,
            configs,
        })
    }
}

impl Deref for Destinations {
    type Target = dyn Destination;

    fn deref(&self) -> &Self::Target {
        match self {
            Mqtt(m) => m,
            Lite(l) => l,
            Http(h) => h,
            Tpc(t) => t,
            #[cfg(test)]
            Dummy(d) => d,
            Postgres(p) => p,
            Mongo(m) => m,
        }
    }
}

impl DerefMut for Destinations {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Mqtt(m) => m,
            Lite(l) => l,
            Http(h) => h,
            Tpc(t) => t,
            #[cfg(test)]
            Dummy(d) => d,
            Postgres(p) => p,
            Mongo(m) => m,
        }
    }
}

pub trait Destination: Send + Configurable + Sync {
    fn parse(options: Map<String, Value>) -> Result<Self, TrackError>
    where
        Self: Sized;

    fn operate(&mut self, id: usize, tx: Tx<Train>, pool: HybridThreadPool) -> Result<usize, TrackError>;

    fn type_(&self) -> String;

    fn dump_destination(&self, _include_id: bool) -> String {
        Configurable::dump(self)
    }

    fn get_configs(&self) -> HashMap<String, ConfigModel>;

    #[cfg(test)]
    fn get_result_handle(&self) -> Arc<Mutex<Vec<Train>>> {
        panic!()
    }
}
