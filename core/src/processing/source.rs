use crate::processing::configuration::Configurable;
use crate::ConfigModel;
use std::collections::HashMap;
use error::error::TrackError;
use threading::multi::MultiSender;
use threading::pool::HybridThreadPool;
use value::train::Train;

pub trait Source: Send + Sync + Configurable {

    fn operate(&mut self, id: usize, outs: MultiSender<Train>, pool: HybridThreadPool) -> Result<usize, TrackError>;

    fn type_(&self) -> String;

    fn dump_source(&self, _include_id: bool) -> String {
        Configurable::dump(self).to_string()
    }

    fn get_configs(&self) -> HashMap<String, ConfigModel>;

}