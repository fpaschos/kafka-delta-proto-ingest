use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use kafka_delta_proto_ingest::start_ingest;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// TODO add clap and cli commands
// TODO add tracing json via cli param see: https://github.com/tokio-rs/tracing/blob/master/examples/examples/toggle-subscribers.rs

const TOPIC: &str = "topic";

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let plain = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry().with(plain).init();


    let cancellation = Arc::new(CancellationToken::new());
    let res = tokio::spawn({
        let cancellation = cancellation.clone();
        async move {
            let res = start_ingest(TOPIC.to_string(), Default::default(), cancellation.clone()).await;
            cancellation.cancel();
            res
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl-C received, stopping ingest");
            cancellation.cancel();
        }
        _ = cancellation.cancelled() => {
            info!("Cancellation token received, stopping ingest");
            // res = run.await.expect("Ingest run failed")
        }
    }

    if let Err(err) =  res.await? {
        error!("Ingest run failed: {:?}", err);
    } else {
        info!("Ingest run finished gracefully");
    }
    Ok(())
}