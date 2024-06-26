mod ingest;
mod deserialize;
mod writer;

use std::sync::Arc;
use rdkafka::{ClientConfig, ClientContext};
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use url::Url;
use crate::ingest::IngestProcessor;

// Re-exports
pub use writer::{DataWriter, record_batch_from_json};

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("Ingest error")]
    IngestError,
}


#[derive(Debug, Clone)]
pub struct IngestOptions {
    /// The Kafka broker string to connect to.
    pub kafka_brokers: String,
    /// The Kafka consumer group id to set to allow for multiple consumers per topic.
    pub consumer_group_id: String,
    /// Input format
    pub input_format: MessageFormat,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            kafka_brokers: "localhost:9092".to_string(),
            consumer_group_id: "kafka-delta-ingest".to_string(),
            input_format: MessageFormat::Protobuf(SchemaSource::None),
        }
    }
}

/// Formats for message parsing
#[derive(Clone, Debug)]
pub enum MessageFormat {
    Protobuf(SchemaSource),
}

#[derive(Clone, Debug)]
pub enum SchemaSource {
    None,
    SchemaRegistry(Url),
}


pub struct KafkaContext;

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {}

pub async fn start_ingest(
    topic: String,
    opts: IngestOptions,
    cancellation_token: Arc<CancellationToken>,
) -> Result<(), IngestError> {
    info!("Starting ingest for topic: {}", topic);

    // TODO separate method kafka config from opts
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer<KafkaContext> = ClientConfig::new()
        .set("group.id", &opts.consumer_group_id)
        .set("bootstrap.servers", &opts.kafka_brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create_with_context(KafkaContext).map_err(|_e| {
        IngestError::IngestError
    })?;

    consumer.subscribe(&[topic.as_str()]).map_err(|_e| {
        IngestError::IngestError
    })?;


    let ingest_processor = IngestProcessor::new(topic, opts)?;

    // The run loop
    loop {
        tokio::select! {
            consumer_result = consumer.recv() => {
                match consumer_result {
                    Ok(message) => {
                        ingest_processor.process_message(message).await?;
                    }
                    Err(e) => {
                        error!("Error while consuming message: {:?}", e);
                    }
                }
            }
            _ = cancellation_token.cancelled() => {
                // TODO clean up if needed
                return Ok(());
            }
        }
    }
}