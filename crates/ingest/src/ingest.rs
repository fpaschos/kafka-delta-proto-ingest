use rdkafka::Message;
use schema_registry_converter::async_impl::proto_decoder::DecodeResultWithContext;
use tracing::{info, trace};
use crate::{IngestError, IngestOptions};
use crate::deserialize::{DeserializeError, OldProtoDeserializer};

pub struct IngestProcessor {
    topic: String,
    opts: IngestOptions,
    deserializer: OldProtoDeserializer,
}

impl IngestProcessor {
    pub fn new(topic: String, opts: IngestOptions) -> Result<Self, IngestError> {
        let deserializer = OldProtoDeserializer::build_from(opts.clone())?;
        Ok(Self {
            topic,
            opts,
            deserializer,
        })
    }

    pub async fn process_message<M>(&self, message: M) -> Result<(), IngestError>
        where M: Message + Send + Sync
    {
        let partition = message.partition();
        let offset = message.offset();
        trace!("Received message from partition {} at offset {}", partition, offset);


        match self.deserialize_message(&message).await {
            Ok(value) => {

                info!("Deserialized message: {:?}", value.full_name);
                let full_name = value.full_name;
                let info = value.context.context.get_message(&full_name).unwrap();
                value.value.fields.iter().for_each(|v| {
                    let field = info.get_field(v.number).unwrap();

                    info!("FieldName: {:?} Value: {:?} ", field.name,  v.value);

                });
            }
            Err(e) => {
                info!("Failed to deserialize message: {:?}", e);
            }
        }
        Ok(())
    }

    pub async fn deserialize_message<M>(&self, message: &M) -> Result<DecodeResultWithContext, DeserializeError>
        where
            M: Message + Send + Sync
    {
        let payload = message.payload().ok_or(DeserializeError::EmptyPayload)?;
        let value = self.deserializer.deserialize(payload).await?;

        Ok(value)
    }
}