use schema_registry_converter::async_impl::easy_proto_decoder::EasyProtoDecoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::error::SRCError;
use crate::{IngestError, IngestOptions, SchemaSource};
use crate::MessageFormat::Protobuf;
use schema_registry_converter::async_impl::proto_decoder::{DecodeResultWithContext};
use schema_registry::{ProtoDecoder};


#[derive(Debug, thiserror::Error)]
pub enum DeserializeError {
    #[error("Kafka message contained empty payload")]
    EmptyPayload,
    #[error("Kafka message proto deserialization failed")]
    ProtoDecodeError(#[from] SRCError),
    #[error("Kafka message proto deserialization failed")]
    ProtoDecodeContextError
}

pub struct ProtoDeserializer {
    decoder: ProtoDecoder,
}



// TODO remove and replace with custom from schema-registry module.
pub struct OldProtoDeserializer {
    decoder: EasyProtoDecoder,
}

impl OldProtoDeserializer {

    pub fn build_from(opts: IngestOptions) -> Result<Self, IngestError> {
        match &opts.input_format {
            Protobuf(SchemaSource::SchemaRegistry(url)) => {
                let sr_settings = SrSettings::new(url.to_string());
                Ok(Self {
                    decoder: EasyProtoDecoder::new(sr_settings),
                })
            },
            _ => {
                Err(IngestError::IngestError)
            }
        }

    }

    pub async fn deserialize(&self, bytes: &[u8]) -> Result<DecodeResultWithContext, DeserializeError> {
        self.decoder.decode_with_context(Some(bytes)).await?.ok_or(DeserializeError::ProtoDecodeContextError)
    }

}


