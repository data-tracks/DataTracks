use crate::mqtt::MqttSource;
use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
#[cfg(test)]
use crate::processing::source::Sources::Dummy;
use crate::processing::source::Sources::{Http, Lite, Mqtt, Tpc};
#[cfg(test)]
use crate::processing::tests::DummySource;
use crate::processing::HttpSource;
use crate::sql::LiteSource;
use crate::tpc::TpcSource;
use crate::ui::ConfigModel;
use crate::util::{HybridThreadPool, Tx};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use track_rails::message_generated::protocol::{Source as FlatSource, SourceArgs};
use value::train::Train;

pub fn parse_source(type_: &str, options: Map<String, Value>) -> Result<Sources, String> {
    let source = match type_.to_ascii_lowercase().as_str() {
        "mqtt" => Mqtt(MqttSource::parse(options)?),
        "sqlite" => Lite(LiteSource::parse(options)?),
        "http" => Http(HttpSource::parse(options)?),
        "tpc" => Tpc(TpcSource::parse(options)?),
        #[cfg(test)]
        "dummy" => Dummy(DummySource::parse(options)?),
        _ => Err(format!("Invalid type: {}", type_))?,
    };
    Ok(source)
}

#[derive(Clone)]
pub enum Sources {
    Mqtt(MqttSource),
    Lite(LiteSource),
    Http(HttpSource),
    Tpc(TpcSource),
    #[cfg(test)]
    Dummy(DummySource),
}

impl Deref for Sources {
    type Target = dyn Source;

    fn deref(&self) -> &Self::Target {
        match self {
            Mqtt(m) => m,
            Lite(s) => s,
            Http(h) => h,
            Tpc(t) => t,
            #[cfg(test)]
            Dummy(d) => d,
        }
    }
}

impl DerefMut for Sources {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Mqtt(m) => m,
            Lite(s) => s,
            Http(h) => h,
            Tpc(t) => t,
            #[cfg(test)]
            Dummy(d) => d,
        }
    }
}

pub trait Source: Send + Sync + Configurable {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized;

    fn operate(
        &mut self,
        pool: HybridThreadPool,
    ) -> usize;

    #[cfg(test)]
    fn operate_test(&mut self) -> (usize, HybridThreadPool) {
        let pool = HybridThreadPool::new();
        let id = self.operate(pool.clone());
        (id, pool)
    }

    fn add_out(&mut self, out: Tx<Train>) {
        self.outs().push(out);
    }

    fn outs(&mut self) -> &mut Vec<Tx<Train>>;

    fn id(&self) -> usize;

    fn type_(&self) -> String;

    fn dump_source(&self, _include_id: bool) -> String {
        Configurable::dump(self).to_string()
    }

    fn flatternize<'a>(&self, builder: &mut FlatBufferBuilder<'a>) -> WIPOffset<FlatSource<'a>> {
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

    fn serialize(&self) -> SourceModel;

    fn from(configs: HashMap<String, ConfigModel>) -> Result<Sources, String>
    where
        Self: Sized;

    fn serialize_default() -> Result<SourceModel, ()>
    where
        Self: Sized;
}
