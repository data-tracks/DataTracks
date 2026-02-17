use anyhow::{bail, Context};
use flume::Sender;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{ClientConfig, Message};
use std::error::Error;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{debug, error, info};
use util::container::Mapping;
use util::{container, InitialMeta};
use value::Value;

const TOPIC: &str = "poly"; // The topic to consume from
const GROUP_ID: &str = "rust-kafka-sink-group"; // Consumer group ID

struct KafkaSink {
    broker: String,
}

impl KafkaSink {
    pub async fn start(
        &mut self,
        sender: Sender<(Value, InitialMeta)>,
    ) -> Result<(), Box<dyn Error>> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", GROUP_ID)
            .set("bootstrap.servers", self.broker.as_str())
            .set("enable.auto.commit", "false") // Set to false to manually commit offsets after sinking
            .set("session.timeout.ms", "6000")
            .set("auto.offset.reset", "earliest") // Start consuming from the beginning if no offset is found
            .create()?;

        // 2. Subscribe to the topic
        consumer.subscribe(&[TOPIC])?;

        info!("Consumer running. Waiting for messages on topic: {}", TOPIC);

        // 3. Start the Consumption Loop
        loop {
            // Asynchronously wait for a message from the stream
            match consumer.recv().await {
                Ok(msg) => {
                    let payload = match msg.payload_view::<str>() {
                        Some(Ok(s)) => s,
                        Some(Err(_)) => {
                            eprintln!("Error deserializing message payload.");
                            continue;
                        }
                        None => {
                            eprintln!("Received message with no payload.");
                            continue;
                        }
                    };

                    debug!("Received message: {:?}", payload);

                    match serde_json::from_str::<SinkRecord>(payload) {
                        Ok(record) => {
                            match sender.send((
                                Value::from(record.value),
                                InitialMeta {
                                    name: Some(record.id),
                                },
                            )) {
                                Ok(_) => {}
                                Err(err) => return Err(err.to_string().into()),
                            };
                            consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async)?;
                        }
                        Err(e) => {
                            error!(
                                "Failed to deserialize JSON: {}. Raw payload: {}",
                                e, payload
                            );
                        }
                    }
                }
                Err(e) => {
                    error!("Kafka error: {}", e);
                }
            }
        }
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct SinkRecord {
    id: String,
    value: String,
}

impl From<SinkRecord> for Value {
    fn from(value: SinkRecord) -> Self {
        value.value.into()
    }
}

#[derive(Clone)]
pub struct Kafka {
    host: String,
    port: u16,
}

impl Kafka {
    pub async fn new(host: &str, port: u16) -> Self {
        Kafka {
            host: host.to_string(),
            port,
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        container::start_container(
            "kafka-mock",
            "apache/kafka:latest",
            vec![Mapping {
                container: 9092,
                host: self.port,
            }],
            None,
        )
        .await?;

        self.check_kafka_cluster_health().await?;
        info!("Kafka mock server started");

        Ok(())
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        container::stop("kafka-mock").await
    }

    pub async fn send_value_doc(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.send(SinkRecord {
            id: "doc".to_string(),
            value: "Success2".to_string(),
        })
        .await?;
        Ok(())
    }

    pub async fn send_value_graph(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.send(SinkRecord {
            id: "graph".to_string(),
            value: "Success2".to_string(),
        })
        .await?;
        Ok(())
    }

    pub async fn send_value_relational(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.send(SinkRecord {
            id: "relational".to_string(),
            value: "Success2".to_string(),
        })
        .await?;
        Ok(())
    }

    pub async fn send(&self, record: SinkRecord) -> Result<(), Box<dyn Error + Send + Sync>> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", format!("{}:{}", self.host, self.port))
            .set("message.timeout.ms", "5000") // Max time to wait for a message to be delivered
            .create()
            .expect("Producer creation failed");

        debug!("Attempting to send message to topic '{}'...", TOPIC);

        let msg = serde_json::to_vec(&record)?;
        // 2. Define the message (FutureRecord)
        // The FutureRecord specifies the target topic, payload (value), and key.
        let record = FutureRecord::to(TOPIC).payload(&msg).key("test");

        // 3. Send the message and await the result
        // The 'send' call enqueues the message and returns a Future.
        let result = producer.send(record, Duration::from_secs(0)).await;

        // 4. Handle the delivery result
        match result {
            Ok(delivery) => {
                debug!(
                    "✅ Message successfully delivered to partition {} at offset {}",
                    delivery.partition, delivery.offset
                );
            }
            Err((kafka_error, _original_message)) => {
                error!("❌ Failed to deliver message: {:?}", kafka_error);
                // The original_message contains the undelivered FutureRecord
            }
        }

        // Explicitly flush the producer queues before the application exits
        producer
            .flush(Duration::from_secs(5))
            .map_err(|err| Box::from(err.to_string()))
    }

    pub(crate) async fn create_topic(&self) -> anyhow::Result<()> {
        let admin_client: AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", format!("{}:{}", self.host, self.port))
            .create()
            .expect("AdminClient creation failed");

        // 2. Define the new topic configuration
        let new_topic = NewTopic::new(TOPIC, 3, TopicReplication::Fixed(1));

        // Add optional topic configuration if needed (e.g., retention.ms)
        // new_topic.set("retention.ms", "86400000");

        // 3. Create the topic
        info!(
            "Attempting to create topic '{}' with {} partitions...",
            TOPIC, 3
        );

        let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

        match admin_client.create_topics(&[new_topic], &options).await {
            Ok(results) => {
                for result in results {
                    match result {
                        Ok(topic_result) => {
                            info!("✅ Topic created successfully: {}", topic_result)
                        }
                        Err((name, error)) => {
                            eprintln!("❌ Failed to create topic '{}': {:?}", name, error);
                            // Check for the 'TopicAlreadyExists' error if you want to ignore it

                            println!("(Topic already exists, which is acceptable in this case).");
                        }
                    }
                }
            }
            Err(e) => eprintln!("❌ Admin operation failed: {:?}", e),
        }
        Ok(())
    }

    async fn check_kafka_cluster_health(&self) -> anyhow::Result<()> {
        let broker = format!("{}:{}", self.host, self.port);
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "health-check-consumer")
            .set("bootstrap.servers", broker)
            .create()
            .context("Consumer creation failed")?;

        // 2. Call fetch_metadata
        // Passing `None` for the topic requests metadata for *all* topics,
        // which reliably forces a broker connection and cluster discovery.
        let timeout = Duration::from_secs(5);
        match consumer.fetch_metadata(None, timeout) {
            Ok(metadata) => {
                // Success: Metadata was retrieved. Check for active brokers.
                if metadata.brokers().is_empty() {
                    bail!("Cluster metadata retrieved but no active brokers found.");
                }
                Ok(())
            }
            Err(e) => bail!("fetch_metadata failed (Timeout/Error): {}", e),
        }
    }
}

pub async fn start(
    joins: &mut JoinSet<()>,
    sender: Sender<(Value, InitialMeta)>,
) -> anyhow::Result<Kafka> {
    let kafka = Kafka::new("localhost", 9092).await;
    kafka.start().await?;
    kafka.create_topic().await?;

    joins.spawn(async move {
        let mut sink = KafkaSink {
            broker: "localhost:9092".to_string(),
        };
        sink.start(sender).await.map_err(|e| e.to_string()).unwrap();
    });

    Ok(kafka)
}
