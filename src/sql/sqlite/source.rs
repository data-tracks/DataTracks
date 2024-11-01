use crate::processing::option::Configurable;
use crate::processing::plan::SourceModel;
use crate::processing::source::Source;
use crate::processing::station::Command;
use crate::processing::Train;
use crate::ui::ConfigModel;
use crate::util::Tx;
use crossbeam::channel::{unbounded, Sender};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use sqlx::{Connection, Row, SqliteConnection};
use sqlx::sqlite::{SqliteColumn, SqliteRow};
use tokio::runtime::Runtime;
use crate::processing::station::Command::{Ready, Stop};

pub struct LiteSource {
    id: i64,
    path: String,
    outs: Vec<Tx<Train>>,
    query: String,
}

impl LiteSource {}

impl Configurable for LiteSource {
    fn get_name(&self) -> String {
        "SQLite".to_string()
    }

    fn get_options(&self) -> Map<String, Value> {
        todo!()
    }
}

impl Source for LiteSource {
    fn parse(options: Map<String, Value>) -> Result<Self, String>
    where
        Self: Sized,
    {
        todo!()
    }

    fn operate(&mut self, control: Arc<Sender<Command>>) -> Sender<Command> {
        let (tx, rx) = unbounded();
        let id = self.id.clone();
        let query = self.query.to_owned();
        let runtime = Runtime::new().unwrap();
        let path = self.path.clone();

        runtime.block_on(async {
            let mut conn = SqliteConnection::connect(&format!("sqlite::{}", path)).await.unwrap();
            control.send(Ready(id)).unwrap();
            loop {
                if let Ok(command) = rx.try_recv() {
                    match command {
                        Stop(_) => break,
                        _ => {}
                    }
                }

                let mut query = sqlx::query(query.as_str());
                let values = query.map(|r:SqliteRow| {
                    let length = r.columns().iter().len();
                    let mut values = vec![length];
                    for index in 0..length {
                        let value =r.try_get_raw(index).unwrap())
                    }
                }).fetch_all(&mut conn).await.unwrap();
            }
        });
        tx
    }

    fn get_outs(&mut self) -> &mut Vec<Tx<Train>> {
        &mut self.outs
    }


    fn get_id(&self) -> i64 {
        self.id
    }

    fn serialize(&self) -> SourceModel {
        todo!()
    }

    fn from(configs: HashMap<String, ConfigModel>) -> Result<Box<dyn Source>, String>
    where
        Self: Sized,
    {
        todo!()
    }

    fn serialize_default() -> Result<SourceModel, ()>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl TryInto<Value> for SqliteRow {
    type Error = String;

    fn try_into(self) -> Result<Value, Self::Error> {
        todo!()
    }
}