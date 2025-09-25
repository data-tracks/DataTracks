use crate::util::{ChangeDataCapture, Event};
use bytes::Bytes;
use futures_util::{Sink, TryStreamExt, ready};
use serde_json::Value;
use std::fmt::{Debug, Display, Formatter};
use std::future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use threading::command::Command;
use threading::multi::MultiSender;
use threading::pool::{HybridThreadPool, WorkerMeta};
use tokio::runtime::Runtime;
use tokio::time::sleep;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{CopyBothDuplex, NoTls, SimpleQueryMessage};
use tracing::{debug, info};
use error::error::TrackError;
use value::train::Train;

const SECONDS_FROM_UNIX_EPOCH_TO_2000: u128 = 946_684_800;

#[derive(Debug, Clone)]
pub struct Identifier {
    pub schema: Option<String>,
    pub table: String,
}

impl Identifier {
    pub fn new<S: AsRef<str>>(schema: Option<String>, table: S) -> Self {
        Identifier { schema, table: table.as_ref().to_string() }
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(schema) = &self.schema {
            f.write_str(schema)?;
            f.write_str(".")?;
        }
        f.write_str(self.table.as_str())
    }
}

/// PostgresCDC Replicator inspired by https://github.com/tablelandnetwork/pglogrepl-rust
pub struct PostgresCdc {
    pub client: postgres::Client,
    name: String,
    table: Option<Identifier>,
    initial_lsn: Option<PgLsn>,
    url: String,
    port: u16,
    watch_init: bool,
}

impl PostgresCdc {
    pub fn new<S: AsRef<str>>(
        url: S,
        port: u16,
        table: Option<Identifier>,
    ) -> Result<Self, String> {
        Self::connect(url.as_ref(), "localhost", port, table)
    }

    pub fn connect<S1: AsRef<str>, S2: AsRef<str>>(
        name: S2,
        url: S1,
        port: u16,
        table: Option<Identifier>,
    ) -> Result<Self, String> {
        let mut client = postgres::Client::connect(
            &format!(
                "host={} port={} user=postgres password=postgres",
                url.as_ref(),
                port
            ),
            postgres::NoTls,
        )
        .map_err(|err| format!("client {}", err.to_string()))?;

        client
            .simple_query("SELECT 1;")
            .map_err(|err| format!("select {}", err.to_string()))?;

        let cdc = PostgresCdc {
            name: name.as_ref().to_string(),
            table,
            initial_lsn: None,
            url: url.as_ref().to_string(),
            client,
            port,
            watch_init: false,
        };

        Ok(cdc)
    }

    fn init(&mut self) -> Result<(), String> {
        match self.create_slot("slot_1") {
            Ok(_) => {}
            Err(err) => println!("slot_1 error {:?}", err),
        };
        self.watch_init = true;
        Ok(())
    }

    fn create_slot<S: AsRef<str>>(&mut self, slot_name: S) -> Result<PgLsn, String> {
        let rt = Runtime::new().unwrap();

        let lsn = rt.block_on(async {
            let slot_query = format!(
                "CREATE_REPLICATION_SLOT {} LOGICAL \"wal2json\" NOEXPORT_SNAPSHOT", // pgoutput
                slot_name.as_ref()
            );

            let (client, connection) = tokio_postgres::connect(
                &format!(
                    "host={} port={} user=postgres password=postgres replication=database",
                    self.url, self.port
                ),
                NoTls,
            )
            .await
            .map_err(|err| err.to_string())?;

            // preforms the communication so we spawn it away
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            info!("Creating slot {}", &slot_query);
            let result = match client.simple_query(&slot_query).await {
                Ok(msg) => msg,
                Err(err) => {
                    //error!("{}, {}", slot_query, err);
                    return Err(format!("Failed to create slot: {}", err));
                }
            };

            let lsn = result
                .into_iter()
                .filter_map(|msg| match msg {
                    SimpleQueryMessage::Row(row) => Some(row),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .first()
                .ok_or_else(|| "No answer")?
                .get("consistent_point")
                .ok_or_else(|| "No field")?
                .to_owned();
            debug!("Created replication slot: {:?}", lsn);
            let lsn = lsn
                .parse::<PgLsn>()
                .map_err(|err| format!("error on parse: {:?}", err))?;

            Ok(lsn)
        });

        if let Ok(lsn) = lsn.clone() {
            self.initial_lsn = Some(lsn);
        }
        lsn
    }
}

impl Debug for PostgresCdc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresCdc")
            .field("url", &self.url)
            .field("port", &self.port)
            .finish()
    }
}

impl Clone for PostgresCdc {
    fn clone(&self) -> Self {
        PostgresCdc::new(self.url.clone(), self.port.clone(), self.table.clone()).unwrap()
    }
}

impl ChangeDataCapture for PostgresCdc {
    fn listen(
        &mut self,
        id: usize,
        outs: MultiSender<Train>,
        pool: HybridThreadPool,
    ) -> Result<usize, TrackError> {
        if !self.watch_init {
            self.init()?;
        }

        let start_lsn = self.initial_lsn.ok_or("Could not connect to postgres")?;

        let url = self.url.clone();
        let port = self.port;

        let target = self.table.clone();

        pool.execute_async("Postgres CDC", move |meta| {
            Box::pin(async move {
                let (client, connection) = tokio_postgres::connect(
                    &format!(
                        "host={} port={} user=postgres password=postgres replication=database",
                        url, port
                    ),
                    NoTls,
                )
                .await
                .map_err(|err| err.to_string())?;

                //let full_table_name = "test";
                let options = vec![
                    ("pretty-print", "true"),
                    ("include-transaction", "true"),
                    ("include-lsn", "true"),
                    ("include-timestamp", "true"),
                    ("include-pk", "true"),
                    ("format-version", "2"),
                    ("include-xids", "true"),
                ];

                let mut options = options
                    .iter()
                    .map(|(k, v)| format!("\"{}\" '{}'", k, v))
                    .collect::<Vec<_>>()
                    .join(", ");

                if let Some(table) = target {
                    options += &format!(", \"{}\" '{}'", "add-tables", table);
                }

                // preforms the communication so we spawn it away
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("connection error: {}", e);
                    }
                });
                let query = format!(
                    "START_REPLICATION SLOT {} LOGICAL {} ({})",
                    "slot_1", start_lsn, options
                );
                let duplex_stream: CopyBothDuplex<Bytes> =
                    client.copy_both_simple(&query).await.unwrap();

                // Pin the stream
                let mut streamer = Streamer {
                    id,
                    counter: 0,
                    part_id: id,
                    stream: Box::pin(duplex_stream),
                    commit_lsn: start_lsn,
                    outs,
                    meta,
                };

                streamer.stream().await;
                Ok(())
            })
        })
    }
}

impl Deref for PostgresCdc {
    type Target = postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for PostgresCdc {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

struct Streamer {
    id: usize,
    counter: usize,
    part_id: usize,
    stream: Pin<Box<CopyBothDuplex<Bytes>>>,
    commit_lsn: PgLsn,
    meta: WorkerMeta,
    outs: MultiSender<Train>,
}

impl Streamer {
    pub(crate) async fn stream(&mut self) {
        self.meta
            .output_channel
            .send(Command::Ready(self.id))
            .unwrap();
        println!("send {}", self.id);
        loop {
            debug!("Postgres streamer running...");
            if self.meta.should_stop() {
                break;
            }

            match self.stream.as_mut().try_next().await {
                Ok(msg) => {
                    match msg {
                        Some(event) => {
                            //println!("{:?}", event);
                            match self.process(event).await {
                                Ok(_) => {}
                                Err(err) => panic!("{:?}", err),
                            };
                        }
                        None => {
                            info!("Stream closed");
                            break;
                        }
                    }
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        debug!("Stream finished");
    }

    async fn send_ssu(&mut self, buf: Bytes) {
        debug!("Trying to send SSU");
        let mut next_step = 1;
        future::poll_fn(|cx| {
            loop {
                match next_step {
                    1 => {
                        ready!(self.stream.as_mut().as_mut().poll_ready(cx)).unwrap();
                    }
                    2 => {
                        self.stream
                            .as_mut()
                            .as_mut()
                            .start_send(buf.clone())
                            .unwrap();
                    }
                    3 => {
                        ready!(self.stream.as_mut().as_mut().poll_flush(cx)).unwrap();
                    }
                    4 => return Poll::Ready(()),
                    _ => panic!(),
                }
                next_step += 1;
            }
        })
        .await;
        debug!("Sent SSU");
    }
    async fn process_record(&mut self, record: Value) {
        match record["action"].as_str().unwrap() {
            "B" => {
                debug!("Begin===");
                debug!("{}", serde_json::to_string_pretty(&record).unwrap());
                let lsn_str = record["nextlsn"].as_str().unwrap();
                self.commit_lsn = lsn_str.parse::<PgLsn>().unwrap();
                match self
                    .outs
                    .send((self.part_id, self.counter, Event::from(record)).into())
                {
                    Ok(_) => {
                        self.counter += 1;
                    }
                    Err(err) => panic!("{:?}", err),
                }
            }
            "C" => {
                let end_lsn_str = record["nextlsn"].as_str().unwrap();
                let end_lsn = end_lsn_str.parse::<PgLsn>().unwrap();
                if end_lsn != self.commit_lsn {
                    println!(
                        "commit and begin next_lsn don't match: {:?}",
                        record["nextlsn"]
                    );
                }
                debug!("Commit===");
                //debug!("{}", serde_json::to_string_pretty(&record).unwrap());

                match self
                    .outs
                    .send((self.part_id, self.counter, Event::from(record)).into())
                {
                    Ok(_) => {
                        self.counter += 1;
                    }
                    Err(err) => println!("Error: {:?}", err),
                };
                match self.commit().await {
                    Ok(_) => {}
                    Err(err) => panic!("Commit error: {}", err),
                };
            }
            "I" | "U" | "D" => {
                debug!("{}", serde_json::to_string_pretty(&record).unwrap());
                match self
                    .outs
                    .send((self.part_id, self.counter, Event::from(record.clone())).into())
                {
                    Ok(_) => {
                        self.counter += 1;
                    }
                    Err(err) => println!("Error: {}", err),
                };
            }
            _ => {
                debug!("unknown message");
                debug!("{}", serde_json::to_string_pretty(&record).unwrap());
            }
        }
    }

    async fn commit(&mut self) -> Result<(), String> {
        let buf = prepare_ssu(self.commit_lsn)?;
        self.send_ssu(buf).await;
        Ok(())
    }

    async fn process(&mut self, event: Bytes) -> Result<(), String> {
        match event[0] {
            b'w' => {
                // first 24 bytes are metadata
                let json: Value =
                    serde_json::from_slice(&event[25..]).map_err(|err| err.to_string())?;
                self.process_record(json).await;
            }
            b'k' => {
                let last_byte = event.last().ok_or(String::from("Empty stream"))?;
                let timeout_imminent = last_byte == &1;
                debug!(
                    "Got keepalive message @timeoutImminent:{}, @LSN:{:x?}",
                    timeout_imminent, self.commit_lsn,
                );
                if timeout_imminent {
                    let buf = prepare_ssu(self.commit_lsn);
                    self.send_ssu(buf?).await;
                }
            }
            _ => (),
        }
        Ok(())
    }
}

fn prepare_ssu(write_lsn: PgLsn) -> Result<Bytes, String> {
    let write_lsn_bytes = u64::from(write_lsn).to_be_bytes();
    let time_since_2000: u64 = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_micros()
        - (SECONDS_FROM_UNIX_EPOCH_TO_2000 * 1000 * 1000))
        .try_into()
        .map_err(|_| "Could not convert time to u64".to_string())?;

    // see here for format details: https://www.postgresql.org/docs/10/protocol-replication.html
    let mut data_to_send: Vec<u8> = vec![];
    // Byte1('r'); Identifies the message as a receiver status update.
    data_to_send.extend_from_slice(&[114]); // "r" in ascii

    // The location of the last WAL byte + 1 received and written to disk in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The location of the last WAL byte + 1 flushed to disk in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The location of the last WAL byte + 1 applied in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    //0, 0, 0, 0, 0, 0, 0, 0,
    data_to_send.extend_from_slice(&time_since_2000.to_be_bytes());
    // Byte1; If 1, the client requests the server to reply to this message immediately. This can be used to ping the server, to test if the connection is still healthy.
    data_to_send.extend_from_slice(&[1]);

    Ok(Bytes::from(data_to_send))
}

#[cfg(test)]
mod tests {
    use crate::Manager;
    use crate::postgres::replication::{Identifier, PostgresCdc};
    use crate::util::{ChangeDataCapture, Container};

    pub fn start_docker_new<S1: AsRef<str>, S2: AsRef<str>>(
        name: S1,
        url: S2,
        port: u16,
        table: Option<Identifier>,
    ) -> Result<PostgresCdc, String> {
        let manager = Manager::new()?;
        manager.init_and_reset_container(name.as_ref(), Container::postgres(url.as_ref(), port))?;

        PostgresCdc::new(url, port, table)
    }

    pub fn new_postgres_cdc<S: AsRef<str>>(name: S, url: S, port: u16) -> Result<PostgresCdc, String> {
        let mut postgres = start_docker_new(
            name.as_ref(),
            url.as_ref(),
            port,
            Some(Identifier {
                schema: Some("public".to_string()),
                table: "test".to_string(),
            }),
        )?;
        postgres
            .client
            .simple_query("CREATE TABLE test (id bigint primary key, val TEXT);")
            .map_err(|err| format!("create {}", err.to_string()))?;
        Ok(postgres)
    }

    #[test]
    fn test_insert() {
        let mut cdc = new_postgres_cdc("post_test_insert", "127.0.0.1", 5556).unwrap();

        let (pool, tx) = cdc.listen_test().unwrap();

        cdc.query("INSERT INTO test VALUES(3, 'testing')", &[])
            .map_err(|err| format!("values insert {}", err.to_string()))
            .unwrap();

        let receiver = tx.subscribe();

        let _value = receiver.recv().unwrap(); // begin
        let value = receiver.recv().unwrap(); // value
        println!("input: {:?}", value);
        let _value = receiver.recv().unwrap(); // end

        drop(pool);

        Manager::new()
            .unwrap()
            .remove_container("post_test_insert")
            .unwrap()
    }

    #[test]
    //#[traced_test]
    fn test_update() {
        let mut cdc = new_postgres_cdc("post_test_update", "127.0.0.1", 5557).unwrap();

        let (pool, tx) = cdc.listen_test().unwrap();

        cdc.query("INSERT INTO test VALUES(3, 'testing')", &[])
            .map_err(|err| format!("values insert {}", err.to_string()))
            .unwrap();
        cdc.execute("UPDATE test SET id = 5 WHERE id = 3;", &[])
            .map_err(|err| format!("values update {}", err.to_string()))
            .unwrap();

        let receiver = tx.subscribe();

        let _value = receiver.recv().unwrap(); // begin
        let value = receiver.recv().unwrap(); // value
        println!("insert: {:?}", value);
        let _value = receiver.recv().unwrap(); // end

        let _value = receiver.recv().unwrap(); // begin
        let value = receiver.recv().unwrap(); // value
        println!("update: {:?}", value);
        let _value = receiver.recv().unwrap(); // end

        drop(pool);

        Manager::new()
            .unwrap()
            .remove_container("post_test_update")
            .unwrap()
    }

    #[test]
    //#[traced_test]
    fn test_delete() {
        let mut cdc = new_postgres_cdc("post_test_delete", "127.0.0.1", 5558).unwrap();

        let (pool, tx) = cdc.listen_test().unwrap();

        let receiver = tx.subscribe();

        cdc.query("INSERT INTO test VALUES(3, 'testing')", &[])
            .map_err(|err| format!("values insert {}", err.to_string()))
            .unwrap();
        cdc.execute("DELETE FROM test WHERE id = 3;", &[])
            .map_err(|err| format!("values delete {}", err.to_string()))
            .unwrap();

        let _value = receiver.recv().unwrap(); // begin
        let value = receiver.recv().unwrap(); // value
        println!("insert: {:?}", value);
        let _value = receiver.recv().unwrap(); // end

        let _value = receiver.recv().unwrap(); // begin
        let value = receiver.recv().unwrap(); // value
        println!("delete: {:?}", value);
        let _value = receiver.recv().unwrap(); // end

        drop(pool);

        Manager::new()
            .unwrap()
            .remove_container("post_test_delete")
            .unwrap()
    }

    #[test]
    //#[traced_test]
    fn test_commit() {
        let mut cdc = new_postgres_cdc("post_test_commit", "127.0.0.1", 5559).unwrap();

        let (pool, tx) = cdc.listen_test().unwrap();

        let mut transaction = cdc.transaction().unwrap();
        transaction
            .execute("INSERT INTO test VALUES(3, 'testing')", &[])
            .map_err(|err| format!("values insert {}", err.to_string()))
            .unwrap();
        transaction
            .execute("INSERT INTO test VALUES(5, 'testing')", &[])
            .map_err(|err| format!("values insert {}", err.to_string()))
            .unwrap();
        transaction.commit().unwrap();

        let receiver = tx.subscribe();

        let _value = receiver.recv().unwrap(); // begin
        let value = receiver.recv().unwrap(); // value
        println!("insert: {:?}", value);
        let value = receiver.recv().unwrap(); // value
        println!("insert: {:?}", value);
        let _value = receiver.recv().unwrap(); // end

        drop(pool);

        Manager::new()
            .unwrap()
            .remove_container("post_test_commit")
            .unwrap()
    }
}
