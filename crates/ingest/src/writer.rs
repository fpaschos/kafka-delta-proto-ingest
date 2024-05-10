use deltalake::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::json::ReaderBuilder;
use deltalake::arrow::record_batch::RecordBatch;
use serde_json::Value as JsonValue;

#[derive(Debug, thiserror::Error)]
pub enum DataWriterError {

    /// Arrow returned an error.
    #[error("Arrow interaction failed: {source}")]
    Arrow {
        /// The wrapped [`ArrowError`]
        #[from]
        source: ArrowError,
    },

    #[error("Unknown generic error")]
    Generic
}


pub struct DataWriter {

}

impl DataWriter {

}

/// Creates an Arrow RecordBatch from the passed JSON buffer.
pub fn record_batch_from_json(
    arrow_schema: ArrowSchemaRef,
    json: &[JsonValue],
) -> Result<RecordBatch, DataWriterError> {
    let mut decoder = ReaderBuilder::new(arrow_schema).build_decoder()?;
    decoder.serialize(json)?;
    decoder
        .flush()?
        .ok_or(DataWriterError::Generic)
}