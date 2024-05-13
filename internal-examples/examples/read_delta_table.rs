use std::sync::Arc;
use deltalake::datafusion::prelude::SessionContext;

/// Example of reading a Delta table using DataFusion
/// This example requires the table to already exist see example `proto-schema-to-delta-table.rs`
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Open and load the latest table state from table_uri.
    // This function errors if the table does not already exist.
    let table = deltalake::open_table("./data/persons").await?;

    let ctx = SessionContext::new();
    ctx.register_table("persons", Arc::new(table))?;

    let df = ctx.sql("SELECT count(*) FROM persons").await?;
    df.show().await?;

    let df = ctx.sql("SELECT * FROM persons").await?;
    df.show_limit(1).await?;


    Ok(())
}