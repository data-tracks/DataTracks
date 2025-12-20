use crate::management::catalog::Catalog;
use crossbeam::channel::{unbounded, Receiver, Sender};
use engine::engine::Engine;
use engine::EngineKind;
use futures::StreamExt;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use num_format::{CustomFormat, Grouping, ToFormattedString};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info};
use util::definition::Definition;
use util::queue::{Meta, RecordContext, RecordQueue};
use value::Value;

pub struct Persister {
    engines: Vec<Engine>,
    catalog: Catalog,
}

impl Persister {
    pub fn new(catalog: Catalog) -> Persister {
        Persister {
            engines: vec![],
            catalog,
        }
    }

    pub async fn next(
        &self,
        value: Value,
        context: RecordContext,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (engine, context) = self.select_engines(&value, context).await?;

        debug!("store {} - {}", engine, value);
        if engine.tx.len() > 10_000 {
            let format = CustomFormat::builder()
                .separator("'")
                .build()?;
            error!("engines {} too long: {}", engine, engine.tx.len().to_formatted_string(&format));
        }
        engine.tx.send((value, context))?;

        Ok(())
    }

    pub async fn start(
        mut self,
        joins: &mut JoinSet<()>,
        mut rx: UnboundedReceiver<(Value, RecordContext)>,
    ) {
        joins.spawn(async move {
            let mut engines = self.catalog.engines().await;

            self.engines.append(&mut engines);

            loop {
                match rx.recv().await {
                    None => {}
                    Some((value, context)) => self.next(value, context).await.unwrap(),
                }
            }
        });
    }

    async fn select_engines(
        &self,
        value: &Value,
        context: RecordContext,
    ) -> Result<(&Engine, RecordContext), Box<dyn Error + Send + Sync>> {
        let definitions = self.catalog.definitions().await;

        let mut definition = Definition::empty();

        for mut d in definitions {
            if d.matches(value, &context.meta) {
                definition = d;
            }
        }

        let costs: Vec<_> = self.engines.iter().map(|e| (e.cost(value, &definition), e)).collect();

        Ok((
            costs.into_iter().min_by(|a, b| a.0.total_cmp(&b.0)).unwrap().1,
            RecordContext {
                meta: context.meta,
                entity: Some(definition.entity),
            },
        ))
    }

    pub async fn start_distributor(&mut self, joins: &mut JoinSet<()>) {
        for mut engine in self.catalog.engines().await {
            let clone = engine.clone();

            joins.spawn(async move {
                let mut error_count = 0;
                let mut count = 0;
                let mut first_ts = Instant::now();
                let mut buckets: HashMap<String, Vec<Value>> = HashMap::new();

                loop {
                    if first_ts.elapsed().as_millis() > 10 || count > 1_000_000 {
                        // try to drain the "buffer"

                        for (entity, values) in buckets.drain() {
                            match clone.store(entity.clone(), values.clone()).await {
                                Ok(_) => {
                                    error_count = 0;
                                    count = 0;
                                }
                                Err(err) => {
                                    error_count += 1;
                                    let errors: Vec<_> = values
                                        .into_iter()
                                        .filter_map(|v| {
                                            let res = engine
                                                .tx
                                                .send((
                                                    v,
                                                    RecordContext::new(
                                                        Meta::new(Some(entity.clone())),
                                                        entity.clone(),
                                                    ),
                                                ))
                                                .map_err(|err| format!("{:?}", err));
                                            match res {
                                                Ok(_) => None,
                                                Err(err) => Some(err)
                                            }
                                        })
                                        .collect();

                                    if !errors.is_empty() {
                                        error!("{}", errors.first().unwrap())
                                    }

                                    if error_count > 1_000 {
                                        error!("Error during distribution to engines over 1'000 tries, {} sleeping longer", err);
                                        sleep(Duration::from_millis(10)).await;
                                    } else if error_count > 10_000 {
                                        error!("Error during distribution to engines over 10'000 tries, {} shut down", err);
                                        panic!("Over 1 Mio retries")
                                    }
                                }
                            }
                        }
                    }


                    match engine.rx.try_recv() {
                        Err(_) => sleep(Duration::from_millis(1)).await, // max shift after max timeout for sending finished chunk out
                        Ok((v, context)) => {
                            let entity = context.entity.unwrap_or("_stream".to_string());

                            if buckets.is_empty() {
                                first_ts = Instant::now();
                            }
                            buckets.entry(entity).or_default().push(v);
                            count += 1;
                        }
                    }
                }
            });
        }
    }
}
