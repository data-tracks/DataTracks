use crate::management::catalog::Catalog;
use crate::phases::Persister;
use crate::phases::nativer::Nativer;
use engine::EngineKind;
use flume::{Sender, unbounded};
use std::thread;
use tokio::runtime::Builder;
use tokio::sync;
use tokio::task::JoinSet;
use tracing::{error, info};
use util::definition::{Definition, DefinitionFilter, Model};
use util::runtimes::Runtimes;
use util::{
    Batch, DefinitionMapping, Event, InitialRecord, RelationalType, TargetedRecord, log_channel,
};

pub struct Manager {
    catalog: Catalog,
    joins: JoinSet<()>,
    runtimes: Runtimes,
    statistic_tx: Sender<Event>,
    output: sync::broadcast::Sender<Batch<TargetedRecord>>,
}

impl Default for Manager {
    fn default() -> Self {
        Self::new()
    }
}

pub type SinkRunner = fn(&mut JoinSet<()>, Sender<InitialRecord>, statistic_tx: Sender<Event>);

impl Manager {
    pub fn new() -> Manager {
        let runtimes = Runtimes::new();
        let (tx, rx) = unbounded::<Event>();

        let rt = runtimes.clone();

        let output = sync::broadcast::channel(10_000).0;

        let statistic_tx = statistics::start(rt, tx, rx, output.clone());

        Self {
            joins: JoinSet::new(),
            catalog: Catalog::new(statistic_tx.clone()),
            runtimes: Runtimes::new(),
            statistic_tx,
            output,
        }
    }

    pub fn start(mut self, sink_runner: SinkRunner) -> anyhow::Result<()> {
        let ctrl_c_signal = tokio::signal::ctrl_c();

        let main_rt = Builder::new_multi_thread()
            .worker_threads(8)
            .thread_name("main-rt")
            .enable_all()
            .build()?;

        let trash_rt = Builder::new_multi_thread()
            .worker_threads(8)
            .thread_name("trash-rt")
            .enable_all()
            .build()?;

        self.runtimes.add_runtime(trash_rt);

        let mut persister = Persister::new(self.catalog.clone())?;
        let nativer = Nativer::new(self.catalog.clone());

        self.init_engines(self.statistic_tx.clone())?;

        let rt = self.runtimes.clone();

        let statistic_tx = self.statistic_tx.clone();

        let output = self.output.clone();

        main_rt.block_on(async move {
            let mut joins: JoinSet<()> = JoinSet::new();

            self.init_definitions(statistic_tx.clone()).await?;

            persister.start_distributor(rt.clone()).await;

            let sink = self.start_sinks(persister, rt).await?;
            //let kafka = sink::kafka::start(&mut self.joins, tx.clone()).await?;

            sink_runner(&mut self.joins, sink, statistic_tx);

            nativer.start(&mut self.joins, output).await;

            tokio::select! {
                    _ = ctrl_c_signal => {
                        info!("#️⃣ Ctrl-C received!");
                    }
                    Some(res) = joins.join_next() => {
                        if let Err(e) = res {
                            error!("\nFatal Error: A core task crashed: {:?}", e);
                        }
                    }
            }

            //info!("Stopping kafka...");
            //kafka.stop().await?;

            info!("Stopping engines...");
            self.catalog.stop().await?;

            // Clean up all remaining running tasks
            info!("🧹 Aborting remaining tasks...");
            joins.abort_all();
            while joins.join_next().await.is_some() {}

            info!("✅ All services shut down. Exiting.");
            anyhow::Ok::<()>(())
        })
    }

    async fn init_definitions(&mut self, statistic_tx: Sender<Event>) -> anyhow::Result<()> {
        self.catalog
            .add_definition(
                Definition::new(
                    "Document test",
                    DefinitionFilter::Topic(String::from("doc")),
                    DefinitionMapping::document(),
                    Model::Document,
                    String::from("doc"),
                )
                .await,
                statistic_tx.clone(),
            )
            .await?;

        self.catalog
            .add_definition(
                Definition::new(
                    "Relational test",
                    DefinitionFilter::Topic(String::from("relational")),
                    DefinitionMapping::tuple_to_relational(vec![
                        ("name".to_string(), RelationalType::Text),
                        ("age".to_string(), RelationalType::Integer),
                    ]),
                    Model::Relational,
                    String::from("relational"),
                )
                .await,
                statistic_tx.clone(),
            )
            .await?;

        self.catalog
            .add_definition(
                Definition::new(
                    "Graph test",
                    DefinitionFilter::Topic(String::from("graph")),
                    DefinitionMapping::doc_to_graph(),
                    Model::Graph,
                    String::from("graph"),
                )
                .await,
                statistic_tx,
            )
            .await?;
        Ok(())
    }

    fn init_engines(&mut self, statistic_tx: Sender<Event>) -> anyhow::Result<()> {
        let mut catalog = self.catalog.clone();

        let (tx, rx) = unbounded();

        let engines = thread::spawn(move || {
            let rt = Builder::new_current_thread()
                .worker_threads(4)
                .thread_name("engine-rt")
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let mut joins: JoinSet<()> = JoinSet::new();

                let engines = EngineKind::start_all(&mut joins, statistic_tx.clone())
                    .await
                    .unwrap();
                for engine in engines.into_iter() {
                    catalog.add_engine(engine).await;
                }
                tx.send_async(true).await.unwrap();

                joins.join_all().await;
            });
        });
        self.runtimes.add_handle(engines);

        rx.recv()?;

        Ok(())
    }

    async fn start_sinks(
        &mut self,
        persister: Persister,
        rt: Runtimes,
    ) -> anyhow::Result<Sender<InitialRecord>> {
        let (tx, rx) = unbounded();

        let (control_tx, control_rx) = unbounded();

        log_channel(tx.clone(), "Sink Input", Some(control_tx)).await;

        persister.start(rx, rt, control_rx);

        Ok(tx)
    }
}
