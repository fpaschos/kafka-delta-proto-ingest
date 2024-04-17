pub mod ingest;

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("Ingest error")]
    IngestError,
}


#[derive(Debug, Clone, Default)]
pub struct IngestOptions {}


pub async fn start_ingest(
    topic: String,
    opts: IngestOptions,
    cancellation_token: Arc<CancellationToken>
) -> Result<(), IngestError> {
    info!("Starting ingest for topic: {}", topic);

    // The run loop
    loop {
        if cancellation_token.is_cancelled() {
            return Ok(());
        }
    }
}