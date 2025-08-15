use crate::mqtt::MqttSource;
use crate::postgres::PostgresSource;
use crate::mongo::MongoDbSource;
use crate::processing::HttpSource;
use crate::processing::source::Sources::{Http, Lite, Mongo, Mqtt, Postgres, Tpc};
#[cfg(test)]
use crate::processing::source::Sources::Dummy;
use crate::sqlite::LiteSource;
use crate::tpc::TpcSource;
use crate::util::new_id;
use core::ConfigModel;
use core::Source;
use core::SourceModel;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use threading::channel::Tx;
use track_rails::message_generated::protocol::{Source as FlatSource, SourceArgs};
use value::train::Train;
#[cfg(test)]
use crate::tests::DummySource;

#[derive(Clone)]
pub struct SourceHolder {
    id: usize,
    sender: Vec<Tx<Train>>,
    source: Sources,
}

impl SourceHolder {
    pub fn new(source: Sources) -> SourceHolder {
        SourceHolder {
            id: new_id(),
            sender: vec![],
            source,
        }
    }
    pub fn add_out(&mut self, out: Tx<Train>) {
        self.sender.push(out);
    }

    pub fn outs(&mut self) -> &mut Vec<Tx<Train>> {
        &mut self.sender
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn flatternize<'a>(
        &self,
        builder: &mut FlatBufferBuilder<'a>,
    ) -> WIPOffset<FlatSource<'a>> {
        let name = Some(builder.create_string(&self.name().to_string()));
        let type_ = Some(builder.create_string(&self.type_().to_string()));

        FlatSource::create(
            builder,
            &SourceArgs {
                id: self.id() as u64,
                name,
                type_,
            },
        )
    }

    pub fn serialize(&self) -> SourceModel {
        let configs = self.get_configs();
        SourceModel {
            type_name: self.type_().to_string(),
            id: self.id.to_string(),
            configs,
        }
    }
}

impl Deref for SourceHolder {
    type Target = Sources;
    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

impl DerefMut for SourceHolder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.source
    }
}

impl TryFrom<(String, Map<String, Value>)> for Sources {
    type Error = String;

    fn try_from(value: (String, Map<String, Value>)) -> Result<Self, Self::Error> {
        let source_type = value.0;
        let options = value.1;
        let source = match source_type.to_lowercase().as_str() {
            "mqtt" => Mqtt(MqttSource::try_from(options)?),
            "sqlite" => Lite(LiteSource::try_from(options)?),
            "http" => Http(HttpSource::try_from(options)?),
            "tpc" => Tpc(TpcSource::try_from(options)?),
            "postgres" | "postgresql" | "pg" => Postgres(PostgresSource::try_from(options)?),
            "mongo" | "mongodb" => Mongo(MongoDbSource::try_from(options)?),
            #[cfg(test)]
            "dummy" => Dummy(DummySource::try_from(options)?),
            _ => Err(format!("Invalid source type: {source_type}"))?,
        };
        Ok(source)
    }
}

impl TryFrom<(String, HashMap<String, ConfigModel>)> for Sources {
    type Error = String;

    fn try_from(value: (String, HashMap<String, ConfigModel>)) -> Result<Self, Self::Error> {
        let source_type = value.0;
        let options = value.1;
        let source = match source_type.to_lowercase().as_str() {
            "mqtt" => Mqtt(MqttSource::try_from(options)?),
            "sqlite" => Lite(LiteSource::try_from(options)?),
            "http" => Http(HttpSource::try_from(options)?),
            "tpc" => Tpc(TpcSource::try_from(options)?),
            "postgres" | "postgresql" | "pg" => Postgres(PostgresSource::try_from(options)?),
            "mongo" | "mongodb" => Mongo(MongoDbSource::try_from(options)?),
            #[cfg(test)]
            "dummy" => Dummy(DummySource::try_from(options)?),
            _ => Err(format!("Invalid source type: {source_type}"))?,
        };
        Ok(source)
    }
}

#[derive(Clone)]
pub enum Sources {
    Mqtt(MqttSource),
    Lite(LiteSource),
    Postgres(PostgresSource),
    Http(HttpSource),
    Tpc(TpcSource),
    Mongo(MongoDbSource),
    #[cfg(test)]
    Dummy(DummySource),
}

impl Sources {
    pub(crate) fn get_default_configs() -> Vec<SourceModel> {
        let values = vec![
            ("MQTT".to_string(), MqttSource::get_default_configs()),
            ("HTTP".to_string(), HttpSource::get_default_configs()),
        ];

        values
            .into_iter()
            .map(|(name, config)| Self::serialize_default(name, config).unwrap())
            .collect()
    }

    fn serialize_default(
        name: String,
        configs: HashMap<String, ConfigModel>,
    ) -> Result<SourceModel, ()>
    where
        Self: Sized,
    {
        Ok(SourceModel {
            type_name: name.clone(),
            id: name,
            configs,
        })
    }
}

impl Deref for Sources {
    type Target = dyn Source;

    fn deref(&self) -> &Self::Target {
        match self {
            Mqtt(m) => m,
            Lite(l) => l,
            Postgres(p) => p,
            Http(h) => h,
            Tpc(t) => t,
            #[cfg(test)]
            Dummy(d) => d,
            Sources::Mongo(m) => m
        }
    }
}

impl DerefMut for Sources {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Mqtt(m) => m,
            Lite(l) => l,
            Postgres(p) => p,
            Http(h) => h,
            Tpc(t) => t,
            #[cfg(test)]
            Dummy(d) => d,
            Sources::Mongo(m) => m
        }
    }
}

impl Into<SourceHolder> for Sources {
    fn into(self) -> SourceHolder {
        #[cfg(test)]
        {
            if let Dummy(d) = self {
                return SourceHolder {
                    id: d.id,
                    sender: vec![],
                    source: Dummy(d),
                };
            }
        }

        SourceHolder::new(self)
    }
}
