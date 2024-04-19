use std::sync::Arc;
use deltalake::datafusion::prelude::SessionContext;


/// Example for learning purposes create table
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Open and load the latest table state from table_uri.
    // This function errors if the table does not already exist.
    let table = deltalake::open_table("./data/claims").await?;

    let ctx = SessionContext::new();
    ctx.register_table("claims", Arc::new(table))?;

    let df = ctx.sql("SELECT * FROM claims").await?;

    let batches = df.collect().await?;

    for (i, batch) in batches.iter().enumerate() {
        println!("Batch {} rows {}",i, batch.num_rows());

    }

    Ok(())
}